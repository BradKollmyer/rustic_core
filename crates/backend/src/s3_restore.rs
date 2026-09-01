//! S3 Glacier RestoreObject / HeadObject sidecar. Never calls OpenDAL `Operator::restore`.

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};
use std::time::SystemTime;

use hmac::{Hmac, Mac};
use log::debug;
use reqwest::header::{HeaderMap, HeaderValue};
use sha2::{Digest, Sha256};

use rustic_core::{ErrorKind, RusticError, RusticResult, WarmupStatus};

use crate::glacier::GlacierConfig;
use crate::reqwest::reqwest_client;

type HmacSha256 = Hmac<Sha256>;

const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// Transport used so RestoreObject/HeadObject can be faked in tests.
pub(crate) trait S3Transport: Send + Sync {
    fn restore(&self, url: &str, headers: HeaderMap, body: &[u8]) -> RusticResult<u16>;
    fn head(&self, url: &str, headers: HeaderMap) -> RusticResult<HeadResult>;
}

#[derive(Debug, Clone, Default)]
pub(crate) struct HeadResult {
    pub status: u16,
    pub storage_class: Option<String>,
    pub restore: Option<String>,
}

struct ReqwestTransport {
    client: reqwest::Client,
}

fn block_on<T>(fut: impl std::future::Future<Output = T>) -> T {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime")
    })
    .block_on(fut)
}

impl S3Transport for ReqwestTransport {
    fn restore(&self, url: &str, headers: HeaderMap, body: &[u8]) -> RusticResult<u16> {
        let response = block_on(
            self.client
                .post(url)
                .headers(headers)
                .body(body.to_vec())
                .send(),
        )
        .map_err(|err| {
            RusticError::with_source(
                ErrorKind::Backend,
                "RestoreObject request to `{url}` failed.",
                err,
            )
            .attach_context("url", url)
        })?;
        Ok(response.status().as_u16())
    }

    fn head(&self, url: &str, headers: HeaderMap) -> RusticResult<HeadResult> {
        let response = block_on(self.client.head(url).headers(headers).send()).map_err(|err| {
            RusticError::with_source(
                ErrorKind::Backend,
                "HeadObject request to `{url}` failed.",
                err,
            )
            .attach_context("url", url)
        })?;
        let status = response.status().as_u16();
        let storage_class = response
            .headers()
            .get("x-amz-storage-class")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let restore = response
            .headers()
            .get("x-amz-restore")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        Ok(HeadResult {
            status,
            storage_class,
            restore,
        })
    }
}

/// Native S3 Glacier restore client.
#[derive(Clone)]
pub(crate) struct S3Restore {
    transport: Arc<dyn S3Transport>,
    bucket: String,
    region: String,
    host: String,
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
    restore_days: i32,
    restore_tier: String,
}

impl std::fmt::Debug for S3Restore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Restore")
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("host", &self.host)
            .field("restore_days", &self.restore_days)
            .field("restore_tier", &self.restore_tier)
            .finish_non_exhaustive()
    }
}

impl S3Restore {
    pub(crate) fn from_options(
        options: &BTreeMap<String, String>,
        glacier: &GlacierConfig,
    ) -> RusticResult<Option<Self>> {
        if !glacier.enable_restore {
            return Ok(None);
        }
        let bucket = options.get("bucket").cloned().ok_or_else(|| {
            RusticError::new(
                ErrorKind::InvalidInput,
                "enable_restore requires a `bucket` option.",
            )
        })?;
        let region = options
            .get("region")
            .cloned()
            .or_else(|| std::env::var("AWS_REGION").ok())
            .or_else(|| std::env::var("AWS_DEFAULT_REGION").ok())
            .unwrap_or_else(|| "us-east-1".to_string());
        let access_key = options
            .get("access_key_id")
            .cloned()
            .or_else(|| std::env::var("AWS_ACCESS_KEY_ID").ok())
            .ok_or_else(|| {
                RusticError::new(
                    ErrorKind::Credentials,
                    "enable_restore needs access_key_id or AWS_ACCESS_KEY_ID. For SSO/assume-role use `--warm-up-command`.",
                )
            })?;
        let secret_key = options
            .get("secret_access_key")
            .cloned()
            .or_else(|| std::env::var("AWS_SECRET_ACCESS_KEY").ok())
            .ok_or_else(|| {
                RusticError::new(
                    ErrorKind::Credentials,
                    "enable_restore needs secret_access_key or AWS_SECRET_ACCESS_KEY.",
                )
            })?;
        let session_token = options
            .get("session_token")
            .cloned()
            .or_else(|| std::env::var("AWS_SESSION_TOKEN").ok());

        let virtual_host = options
            .get("enable_virtual_host_style")
            .is_some_and(|v| v == "true" || v == "1");
        let endpoint = options.get("endpoint").cloned();
        let host = match &endpoint {
            Some(ep) => ep
                .trim_start_matches("https://")
                .trim_start_matches("http://")
                .trim_end_matches('/')
                .to_string(),
            None if virtual_host => format!("{bucket}.s3.{region}.amazonaws.com"),
            None => format!("s3.{region}.amazonaws.com"),
        };

        let client = reqwest_client(options)?;
        Ok(Some(Self {
            transport: Arc::new(ReqwestTransport { client }),
            bucket,
            region,
            host,
            access_key,
            secret_key,
            session_token,
            restore_days: glacier.restore_days,
            restore_tier: glacier.restore_tier.clone(),
        }))
    }

    pub(crate) fn restore_key(&self, key: &str) -> RusticResult<()> {
        if self.restore_tier.eq_ignore_ascii_case("Expedited") {
            // Deep Archive rejects Expedited; still send and surface AWS's error if so.
        }
        let body = format!(
            "<RestoreRequest><Days>{}</Days><GlacierJobParameters><Tier>{}</Tier></GlacierJobParameters></RestoreRequest>",
            self.restore_days, self.restore_tier
        );
        let url = self.object_url(key, Some("restore"));
        let headers = self.sign("POST", key, Some("restore"), body.as_bytes())?;
        let status = self.transport.restore(&url, headers, body.as_bytes())?;
        match status {
            200 | 202 => Ok(()),
            409 => {
                debug!("RestoreAlreadyInProgress for {key}");
                Ok(())
            }
            other => Err(RusticError::new(
                ErrorKind::Backend,
                "RestoreObject for `{key}` returned HTTP {status}. For Deep Archive do not use Expedited.",
            )
            .attach_context("key", key)
            .attach_context("status", other.to_string())),
        }
    }

    pub(crate) fn status_key(&self, key: &str) -> RusticResult<WarmupStatus> {
        let url = self.object_url(key, None);
        let headers = self.sign("HEAD", key, None, b"")?;
        let head = self.transport.head(&url, headers)?;
        if head.status == 404 {
            return Ok(WarmupStatus::Warm);
        }
        Ok(status_from_headers(
            head.storage_class.as_deref(),
            head.restore.as_deref(),
        ))
    }

    fn object_url(&self, key: &str, query: Option<&str>) -> String {
        let key = key.trim_start_matches('/');
        let q = query.map(|q| format!("?{q}")).unwrap_or_default();
        if self.host.starts_with(&self.bucket) || self.host.contains(&format!("{}.s3", self.bucket))
        {
            format!("https://{}/{key}{q}", self.host)
        } else {
            format!("https://{}/{}/{key}{q}", self.host, self.bucket)
        }
    }

    fn canonical_uri(&self, key: &str) -> String {
        let key = key.trim_start_matches('/');
        if self.host.starts_with(&self.bucket) || self.host.contains(&format!("{}.s3", self.bucket))
        {
            format!("/{key}")
        } else {
            format!("/{}/{key}", self.bucket)
        }
    }

    fn sign(
        &self,
        method: &str,
        key: &str,
        query: Option<&str>,
        body: &[u8],
    ) -> RusticResult<HeaderMap> {
        let now = SystemTime::now();
        let datetime = httpdate_like(now);
        let datestamp = &datetime[..8];
        let payload_hash = hex::encode(Sha256::digest(body));
        let payload_hash = if body.is_empty() {
            EMPTY_SHA256.to_string()
        } else {
            payload_hash
        };

        let canonical_uri = self.canonical_uri(key);
        let canonical_query = query.unwrap_or("");
        let mut signed_headers = vec!["host", "x-amz-content-sha256", "x-amz-date"];
        if self.session_token.is_some() {
            signed_headers.push("x-amz-security-token");
        }
        signed_headers.sort_unstable();
        let signed = signed_headers.join(";");

        let mut canonical_headers = format!(
            "host:{}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{datetime}\n",
            self.host
        );
        if let Some(token) = &self.session_token {
            canonical_headers.push_str(&format!("x-amz-security-token:{token}\n"));
        }

        let canonical_request = format!(
            "{method}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed}\n{payload_hash}"
        );
        let canonical_hash = hex::encode(Sha256::digest(canonical_request.as_bytes()));
        let scope = format!("{datestamp}/{}/s3/aws4_request", self.region);
        let string_to_sign = format!("AWS4-HMAC-SHA256\n{datetime}\n{scope}\n{canonical_hash}");
        let signing_key = aws4_signing_key(&self.secret_key, datestamp, &self.region, "s3")?;
        let signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes())?);
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed}, Signature={signature}",
            self.access_key
        );

        let mut headers = HeaderMap::new();
        _ = headers.insert("authorization", hv(&authorization)?);
        _ = headers.insert("x-amz-date", hv(&datetime)?);
        _ = headers.insert("x-amz-content-sha256", hv(&payload_hash)?);
        _ = headers.insert("host", hv(&self.host)?);
        if let Some(token) = &self.session_token {
            _ = headers.insert("x-amz-security-token", hv(token)?);
        }
        Ok(headers)
    }
}

fn hv(value: &str) -> RusticResult<HeaderValue> {
    HeaderValue::from_str(value).map_err(|err| {
        RusticError::with_source(ErrorKind::Internal, "Invalid HTTP header value.", err)
    })
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> RusticResult<Vec<u8>> {
    let mut mac = HmacSha256::new_from_slice(key)
        .map_err(|err| RusticError::with_source(ErrorKind::Internal, "HMAC key error.", err))?;
    mac.update(data);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn aws4_signing_key(
    secret: &str,
    date: &str,
    region: &str,
    service: &str,
) -> RusticResult<Vec<u8>> {
    let k_date = hmac_sha256(format!("AWS4{secret}").as_bytes(), date.as_bytes())?;
    let k_region = hmac_sha256(&k_date, region.as_bytes())?;
    let k_service = hmac_sha256(&k_region, service.as_bytes())?;
    hmac_sha256(&k_service, b"aws4_request")
}

fn httpdate_like(now: SystemTime) -> String {
    let dur = now
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    // YYYYMMDDTHHMMSSZ via a simple UTC breakdown
    let secs = dur.as_secs();
    let days = secs / 86400;
    let rem = secs % 86400;
    let hour = rem / 3600;
    let min = (rem % 3600) / 60;
    let sec = rem % 60;
    let (year, month, day) = civil_from_days(days);
    format!("{year:04}{month:02}{day:02}T{hour:02}{min:02}{sec:02}Z")
}

fn civil_from_days(z: u64) -> (i32, u32, u32) {
    // Howard Hinnant civil_from_days, unix epoch z=0 is 1970-01-01
    let z = i64::try_from(z).unwrap_or(0) + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m as u32, d as u32)
}

pub(crate) fn status_from_headers(
    storage_class: Option<&str>,
    restore: Option<&str>,
) -> WarmupStatus {
    if let Some(restore) = restore {
        let ongoing = restore.contains("ongoing-request=\"true\"");
        if ongoing {
            return WarmupStatus::Warming;
        }
        if restore.contains("ongoing-request=\"false\"") {
            return WarmupStatus::Warm;
        }
    }
    match storage_class.map(str::to_ascii_uppercase).as_deref() {
        Some("GLACIER" | "DEEP_ARCHIVE") => WarmupStatus::Cold,
        _ => WarmupStatus::Warm,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glacier_without_restore_header_is_cold() {
        assert_eq!(
            status_from_headers(Some("GLACIER"), None),
            WarmupStatus::Cold
        );
        assert_eq!(
            status_from_headers(Some("DEEP_ARCHIVE"), None),
            WarmupStatus::Cold
        );
    }

    #[test]
    fn restore_in_progress_is_warming() {
        assert_eq!(
            status_from_headers(Some("GLACIER"), Some("ongoing-request=\"true\"")),
            WarmupStatus::Warming
        );
    }

    #[test]
    fn restored_copy_is_warm() {
        assert_eq!(
            status_from_headers(
                Some("GLACIER"),
                Some("ongoing-request=\"false\", expiry-date=\"Fri, 01 Jan 2027 00:00:00 GMT\"")
            ),
            WarmupStatus::Warm
        );
    }

    #[test]
    fn standard_and_ir_are_warm() {
        assert_eq!(
            status_from_headers(Some("STANDARD"), None),
            WarmupStatus::Warm
        );
        assert_eq!(
            status_from_headers(Some("GLACIER_IR"), None),
            WarmupStatus::Warm
        );
        assert_eq!(status_from_headers(None, None), WarmupStatus::Warm);
    }
}
