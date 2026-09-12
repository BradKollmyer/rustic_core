//! Native Storj Uplink backend for rustic.
use std::{collections::BTreeMap, future::Future, str::FromStr, sync::Arc, time::Duration};

use backon::{BlockingRetryable, ExponentialBuilder};
use bytes::Bytes;
use futures_util::StreamExt;
use log::{trace, warn};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Semaphore;

use rustic_core::{
    BytesList, CommandInput, ErrorKind, FileType, Id, ReadBackend, RusticError, RusticResult,
    WriteBackend,
};

use crate::runtime::runtime;
use crate::util::BackendLocation;

mod constants {
    /// Default number of retries for transient Storj errors.
    pub(super) const DEFAULT_RETRY: usize = 5;
    /// Concurrent Storj object downloads/uploads. Restore otherwise opens 20
    /// pack reads, each fanning out to ~k+1 storage nodes.
    pub(super) const DEFAULT_CONNECTIONS: usize = 5;
    /// Per-read/write SN deadline. SDK default is 10 minutes; a silent node
    /// then stalls long-tail until that expires. 20s is enough for a 64 KiB
    /// read and lets the next piece start.
    pub(super) const DEFAULT_MESSAGE_TIMEOUT_SECS: u64 = 20;
}

/// Native Storj backend.
///
/// Repository URL: `storj:<bucket>` or `storj:<bucket>/<prefix>`.
/// Credentials: `access`, `access-file`, `access-command`, or `STORJ_ACCESS`.
#[derive(Clone)]
pub struct StorjBackend {
    project: storj::Project,
    bucket: String,
    /// Object-key prefix, empty or ending with `/`.
    prefix: String,
    backoff: ExponentialBuilder,
    connections: usize,
    io_limit: Arc<Semaphore>,
}

impl std::fmt::Debug for StorjBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorjBackend")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("connections", &self.connections)
            .finish_non_exhaustive()
    }
}

impl StorjBackend {
    /// Create a new Storj backend.
    ///
    /// # Errors
    ///
    /// * If the location, access grant, or options are invalid.
    /// * If the satellite cannot be reached.
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(location: BackendLocation, options: BTreeMap<String, String>) -> RusticResult<Self> {
        let (bucket, prefix) = parse_bucket_and_prefix(location.as_ref(), &options)?;
        let grant = load_grant(&options)?;
        let access = storj::Access::parse(grant.trim()).map_err(|err| {
            map_storj_error(err).prepend_guidance_line(
                "Failed to parse the Storj access grant. Check `access`, `access-file`, `access-command`, or `STORJ_ACCESS`.",
            )
        })?;

        let user_agent = options
            .get("user-agent")
            .cloned()
            .unwrap_or_else(|| "rustic".to_string());
        let transport = parse_transport(&options)?;
        let connections = parse_connections(&options)?;
        let message_timeout = parse_message_timeout(&options)?;
        let download_hedge_delay = parse_download_hedge_delay(&options)?;
        let config = storj::Config {
            user_agent: Some(user_agent),
            transport,
            message_timeout: Some(message_timeout),
            download_hedge_delay,
            concurrent_segments: Some(connections),
            ..storj::Config::default()
        };

        let backoff = parse_retry(&options)?;

        let project = runtime()
            .block_on(storj::Project::open_with_config(&access, config))
            .map_err(map_storj_error)?;

        Ok(Self {
            project,
            bucket,
            prefix,
            backoff,
            connections,
            io_limit: Arc::new(Semaphore::new(connections)),
        })
    }

    fn object_key(&self, tpe: FileType, id: &Id) -> String {
        object_key(&self.prefix, tpe, id)
    }

    fn retry<T, F>(&self, op: F) -> RusticResult<T>
    where
        F: FnMut() -> Result<T, storj::Error>,
    {
        op.retry(self.backoff)
            .when(is_retryable)
            .notify(|err, duration| warn!("Storj error {err} at {duration:?}, retrying"))
            .call()
            .map_err(map_storj_error)
    }

    fn block_on_retry<T, Fut, F>(&self, mut op: F) -> RusticResult<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, storj::Error>>,
    {
        let io_limit = Arc::clone(&self.io_limit);
        self.retry(|| {
            let _permit = runtime()
                .block_on(io_limit.acquire())
                .expect("Storj I/O semaphore closed");
            runtime().block_on(op())
        })
    }
}

/// Split `storj:` location and options into bucket + prefix.
///
/// Location is `bucket` or `bucket/prefix`. `bucket` option overrides the URL
/// bucket. `root` is an extra prefix under the URL prefix.
pub(crate) fn parse_bucket_and_prefix(
    location: &str,
    options: &BTreeMap<String, String>,
) -> RusticResult<(String, String)> {
    let location = location.trim().trim_matches('/');
    let (url_bucket, url_rest) = match location.split_once('/') {
        Some((bucket, rest)) => (bucket, rest.trim_matches('/')),
        None => (location, ""),
    };

    let bucket = options
        .get("bucket")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| url_bucket.to_string());

    if bucket.is_empty() {
        return Err(RusticError::new(
            ErrorKind::InvalidInput,
            "Storj backend requires a bucket in the repository URL (`storj:<bucket>`) or the `bucket` option.",
        ));
    }
    if bucket.contains('/') {
        return Err(RusticError::new(
            ErrorKind::InvalidInput,
            "Storj bucket name `{bucket}` must not contain `/`.",
        )
        .attach_context("bucket", bucket));
    }

    let root = options
        .get("root")
        .map_or("", String::as_str)
        .trim()
        .trim_matches('/');
    let prefix = [root, url_rest]
        .into_iter()
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("/");
    let prefix = if prefix.is_empty() {
        String::new()
    } else {
        format!("{prefix}/")
    };

    Ok((bucket, prefix))
}

/// Load a serialized access grant from options or `STORJ_ACCESS`.
pub(crate) fn load_grant(options: &BTreeMap<String, String>) -> RusticResult<String> {
    let access = options
        .get("access")
        .map(String::as_str)
        .filter(|s| !s.is_empty());
    let access_file = options
        .get("access-file")
        .map(String::as_str)
        .filter(|s| !s.is_empty());
    let access_command = options
        .get("access-command")
        .map(String::as_str)
        .filter(|s| !s.is_empty());

    let set = [
        access.is_some(),
        access_file.is_some(),
        access_command.is_some(),
    ]
    .into_iter()
    .filter(|x| *x)
    .count();
    if set > 1 {
        return Err(RusticError::new(
            ErrorKind::InvalidInput,
            "Set only one of `access`, `access-file`, or `access-command`.",
        ));
    }

    if let Some(grant) = access {
        return Ok(grant.to_string());
    }
    if let Some(path) = access_file {
        return std::fs::read_to_string(path).map_err(|err| {
            RusticError::with_source(
                ErrorKind::InvalidInput,
                "Cannot read Storj access grant from `{path}`.",
                err,
            )
            .attach_context("path", path.to_string())
        });
    }
    if let Some(cmd) = access_command {
        let command: CommandInput = cmd.parse().map_err(|err| {
            RusticError::with_source(
                ErrorKind::InvalidInput,
                "Cannot parse `access-command` `{command}`.",
                err,
            )
            .attach_context("command", cmd.to_string())
        })?;
        let output = command.stdout()?;
        return String::from_utf8(output).map_err(|err| {
            RusticError::with_source(
                ErrorKind::InvalidInput,
                "`access-command` did not print valid UTF-8.",
                err,
            )
        });
    }
    if let Ok(grant) = std::env::var("STORJ_ACCESS")
        && !grant.trim().is_empty()
    {
        return Ok(grant);
    }

    Err(RusticError::new(
        ErrorKind::MissingInput,
        "No Storj access grant given. Set `access`, `access-file`, `access-command`, or the `STORJ_ACCESS` environment variable.",
    ))
}

/// Restic-layout object key under `prefix`.
pub(crate) fn object_key(prefix: &str, tpe: FileType, id: &Id) -> String {
    let hex = id.to_hex();
    let hex_id = hex.as_str();
    let rel = match tpe {
        FileType::Config => "config".to_string(),
        FileType::Pack => format!("data/{}/{}", &hex_id[0..2], hex_id),
        _ => format!("{}/{hex_id}", tpe.dirname()),
    };
    format!("{prefix}{rel}")
}

/// Storj wire transport.
///
/// The `storj` crate and restic's Go uplink both default to Noise for
/// storage-node transfers. `tcp` / `quic` / `auto` override that. `auto`
/// races QUIC then TCP/TLS and does not use Noise. Network extras (early
/// data, Fast Open, QoS) stay on crate/Go defaults.
fn parse_transport(options: &BTreeMap<String, String>) -> RusticResult<storj::TransportMode> {
    let Some(value) = options.get("transport") else {
        return Ok(storj::TransportMode::Noise);
    };
    match value.to_ascii_lowercase().as_str() {
        "tcp" => Ok(storj::TransportMode::Tcp),
        "quic" => Ok(storj::TransportMode::Quic),
        "auto" => Ok(storj::TransportMode::Auto),
        "noise" => Ok(storj::TransportMode::Noise),
        _ => Err(RusticError::new(
            ErrorKind::InvalidInput,
            "Invalid value `{value}` for option `{option}`. Allowed: tcp, quic, auto, noise.",
        )
        .attach_context("value", value.clone())
        .attach_context("option", "transport")),
    }
}

fn parse_connections(options: &BTreeMap<String, String>) -> RusticResult<usize> {
    match options.get("connections") {
        None => Ok(constants::DEFAULT_CONNECTIONS),
        Some(value) => {
            let connections = usize::from_str(value).map_err(|err| {
                RusticError::with_source(
                    ErrorKind::InvalidInput,
                    "Cannot parse value `{value}`, invalid value for option `{option}`.",
                    err,
                )
                .attach_context("value", value.clone())
                .attach_context("option", "connections")
            })?;
            if connections == 0 {
                return Err(RusticError::new(
                    ErrorKind::InvalidInput,
                    "Backend connections must be greater than zero.",
                ));
            }
            Ok(connections)
        }
    }
}

fn parse_message_timeout(options: &BTreeMap<String, String>) -> RusticResult<Duration> {
    match options.get("message-timeout") {
        None => Ok(Duration::from_secs(constants::DEFAULT_MESSAGE_TIMEOUT_SECS)),
        Some(value) => {
            let secs = u64::from_str(value).map_err(|err| {
                RusticError::with_source(
                    ErrorKind::InvalidInput,
                    "Cannot parse value `{value}`, invalid value for option `{option}`.",
                    err,
                )
                .attach_context("value", value.clone())
                .attach_context("option", "message-timeout")
            })?;
            if secs == 0 {
                return Err(RusticError::new(
                    ErrorKind::InvalidInput,
                    "Storj `message-timeout` must be greater than zero seconds.",
                ));
            }
            Ok(Duration::from_secs(secs))
        }
    }
}

fn parse_download_hedge_delay(
    options: &BTreeMap<String, String>,
) -> RusticResult<Option<Duration>> {
    options
        .get("download-hedge-delay")
        .map(|value| {
            u64::from_str(value)
                .map(Duration::from_secs)
                .map_err(|err| {
                    RusticError::with_source(
                        ErrorKind::InvalidInput,
                        "Cannot parse value `{value}`, invalid value for option `{option}`.",
                        err,
                    )
                    .attach_context("value", value.clone())
                    .attach_context("option", "download-hedge-delay")
                })
        })
        .transpose()
}

fn parse_retry(options: &BTreeMap<String, String>) -> RusticResult<ExponentialBuilder> {
    let mut backoff = ExponentialBuilder::default()
        .with_max_delay(Duration::MAX)
        .with_max_times(constants::DEFAULT_RETRY);

    if let Some(value) = options.get("retry") {
        let max_retries = match value.as_str() {
            "false" | "off" => 0,
            "default" => constants::DEFAULT_RETRY,
            _ => usize::from_str(value).map_err(|err| {
                RusticError::with_source(
                    ErrorKind::InvalidInput,
                    "Cannot parse value `{value}`, invalid value for option `{option}`.",
                    err,
                )
                .attach_context("value", value.clone())
                .attach_context("option", "retry")
            })?,
        };
        backoff = backoff.with_max_times(max_retries);
    }
    Ok(backoff)
}

fn is_retryable(err: &storj::Error) -> bool {
    // Protocol also covers permanent range validation failures. Preserve the
    // SDK's per-cause decision, including aggregated piece download failures.
    err.is_retryable()
}

fn map_storj_error(err: storj::Error) -> Box<RusticError> {
    let kind = match err.kind() {
        storj::ErrorKind::InvalidGrant
        | storj::ErrorKind::BucketNameInvalid
        | storj::ErrorKind::ObjectKeyInvalid => ErrorKind::InvalidInput,
        storj::ErrorKind::PermissionDenied | storj::ErrorKind::DecryptionFailed => {
            ErrorKind::Credentials
        }
        storj::ErrorKind::Io => ErrorKind::InputOutput,
        _ => ErrorKind::Backend,
    };
    let storj_kind = format!("{:?}", err.kind());
    let message = err.to_string();
    RusticError::with_source(kind, "Storj backend error: `{error}`", err)
        .attach_context("error", message)
        .attach_context("storj_kind", storj_kind)
}

fn is_not_found(err: &storj::Error) -> bool {
    matches!(
        err.kind(),
        storj::ErrorKind::ObjectNotFound | storj::ErrorKind::BucketNotFound
    )
}

fn id_from_object_key(key: &str, tpe: FileType) -> Option<Id> {
    let name = key.rsplit('/').next().unwrap_or(key);
    Id::parse_some(name, tpe)
}

impl ReadBackend for StorjBackend {
    fn connection_limit(&self) -> Option<usize> {
        Some(self.connections)
    }

    fn location(&self) -> String {
        if self.prefix.is_empty() {
            format!("storj:{}", self.bucket)
        } else {
            format!(
                "storj:{}/{}",
                self.bucket,
                self.prefix.trim_end_matches('/')
            )
        }
    }

    fn list_with_size(&self, tpe: FileType) -> RusticResult<Vec<(Id, u32)>> {
        trace!("listing tpe: {tpe:?}");
        if tpe == FileType::Config {
            return self.stat_config_size();
        }

        let prefix = format!("{}{}/", self.prefix, tpe.dirname());
        let opts = storj::ListObjectsOptions {
            prefix,
            recursive: true,
            system: true,
            ..storj::ListObjectsOptions::default()
        };

        self.block_on_retry(|| {
            let project = self.project.clone();
            let bucket = self.bucket.clone();
            let opts = opts.clone();
            async move {
                let mut stream = project.list_objects(&bucket, opts);
                let mut out = Vec::new();
                while let Some(item) = stream.next().await {
                    let obj = item?;
                    if obj.is_prefix {
                        continue;
                    }
                    if let Some(id) = id_from_object_key(&obj.key, tpe) {
                        let size = u32::try_from(obj.system.content_length.max(0)).unwrap_or(0);
                        out.push((id, size));
                    }
                }
                Ok(out)
            }
        })
    }

    fn read_full(&self, tpe: FileType, id: &Id) -> RusticResult<Bytes> {
        trace!("reading tpe: {tpe:?}, id: {id}");
        self.read_range(tpe, id, &storj::DownloadOptions::default())
    }

    fn read_partial(
        &self,
        tpe: FileType,
        id: &Id,
        _cacheable: bool,
        offset: u32,
        length: u32,
    ) -> RusticResult<Bytes> {
        trace!("reading partial tpe: {tpe:?}, id: {id}, offset: {offset}, length: {length}");
        self.read_range(
            tpe,
            id,
            &storj::DownloadOptions {
                offset: i64::from(offset),
                length: i64::from(length),
                ..storj::DownloadOptions::default()
            },
        )
    }

    fn warmup_path(&self, tpe: FileType, id: &Id) -> String {
        self.object_key(tpe, id)
    }
}

impl StorjBackend {
    fn stat_config_size(&self) -> RusticResult<Vec<(Id, u32)>> {
        let key = self.object_key(FileType::Config, &Id::default());
        self.retry(|| {
            runtime().block_on(async {
                match self.project.stat_object(&self.bucket, &key).await {
                    Ok(obj) => {
                        let size = u32::try_from(obj.system.content_length.max(0)).unwrap_or(0);
                        Ok(vec![(Id::default(), size)])
                    }
                    Err(err) if is_not_found(&err) => Ok(Vec::new()),
                    Err(err) => Err(err),
                }
            })
        })
    }

    fn read_range(
        &self,
        tpe: FileType,
        id: &Id,
        opts: &storj::DownloadOptions,
    ) -> RusticResult<Bytes> {
        let key = self.object_key(tpe, id);
        self.block_on_retry(|| {
            let project = self.project.clone();
            let bucket = self.bucket.clone();
            let key = key.clone();
            let opts = opts.clone();
            async move {
                let mut download = project.download_object(&bucket, &key, opts).await?;
                let mut buf = Vec::new();
                let _n = download.read_to_end(&mut buf).await?;
                download.close().await?;
                Ok(Bytes::from(buf))
            }
        })
    }
}

impl WriteBackend for StorjBackend {
    fn create(&self) -> RusticResult<()> {
        trace!("ensuring bucket {}", self.bucket);
        self.block_on_retry(|| {
            let project = self.project.clone();
            let bucket = self.bucket.clone();
            async move {
                let _bucket = project.ensure_bucket(&bucket).await?;
                Ok(())
            }
        })
    }

    fn write_bytes(
        &self,
        tpe: FileType,
        id: &Id,
        _cacheable: bool,
        content: BytesList,
    ) -> RusticResult<()> {
        trace!("writing tpe: {tpe:?}, id: {id}");
        let key = self.object_key(tpe, id);
        self.block_on_retry(|| {
            let project = self.project.clone();
            let bucket = self.bucket.clone();
            let key = key.clone();
            let chunks = content.clone();
            async move {
                let mut upload = project
                    .upload_object(&bucket, &key, storj::UploadOptions::default())
                    .await?;
                for chunk in chunks.into_vec() {
                    if let Err(err) = upload.write_all(&chunk).await {
                        let _ = upload.abort().await;
                        return Err(storj::Error::from(err));
                    }
                }
                let _obj = upload.commit().await?;
                Ok(())
            }
        })
    }

    fn remove(&self, tpe: FileType, id: &Id, _cacheable: bool) -> RusticResult<()> {
        trace!("removing tpe: {tpe:?}, id: {id}");
        let key = self.object_key(tpe, id);
        self.retry(|| {
            runtime().block_on(async {
                match self.project.delete_object(&self.bucket, &key).await {
                    Ok(_) => Ok(()),
                    Err(err) if is_not_found(&err) => Ok(()),
                    Err(err) => Err(err),
                }
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn parse_bucket_only() {
        let (bucket, prefix) = parse_bucket_and_prefix("backups", &BTreeMap::new()).unwrap();
        assert_eq!(bucket, "backups");
        assert_eq!(prefix, "");
    }

    #[test]
    fn parse_bucket_and_path() {
        let (bucket, prefix) = parse_bucket_and_prefix("backups/home", &BTreeMap::new()).unwrap();
        assert_eq!(bucket, "backups");
        assert_eq!(prefix, "home/");
    }

    #[test]
    fn parse_root_and_url_prefix() {
        let mut options = BTreeMap::new();
        _ = options.insert("root".into(), "/rustic".into());
        let (bucket, prefix) = parse_bucket_and_prefix("backups/home", &options).unwrap();
        assert_eq!(bucket, "backups");
        assert_eq!(prefix, "rustic/home/");
    }

    #[test]
    fn parse_bucket_option_override() {
        let mut options = BTreeMap::new();
        _ = options.insert("bucket".into(), "other".into());
        let (bucket, prefix) = parse_bucket_and_prefix("backups/home", &options).unwrap();
        assert_eq!(bucket, "other");
        assert_eq!(prefix, "home/");
    }

    #[test]
    fn parse_empty_location_needs_bucket_option() {
        assert!(parse_bucket_and_prefix("", &BTreeMap::new()).is_err());
    }

    #[test]
    fn parse_transport_defaults_to_noise() {
        assert_eq!(
            parse_transport(&BTreeMap::new()).unwrap(),
            storj::TransportMode::Noise
        );
    }

    #[test]
    fn parse_transport_values() {
        for (value, expected) in [
            ("tcp", storj::TransportMode::Tcp),
            ("TCP", storj::TransportMode::Tcp),
            ("quic", storj::TransportMode::Quic),
            ("auto", storj::TransportMode::Auto),
            ("noise", storj::TransportMode::Noise),
        ] {
            let mut options = BTreeMap::new();
            _ = options.insert("transport".into(), value.into());
            assert_eq!(parse_transport(&options).unwrap(), expected);
        }
    }

    #[test]
    fn parse_transport_rejects_unknown() {
        let mut options = BTreeMap::new();
        _ = options.insert("transport".into(), "udp".into());
        assert!(parse_transport(&options).is_err());
    }

    #[test]
    fn parse_connections_defaults_to_five() {
        assert_eq!(
            parse_connections(&BTreeMap::new()).unwrap(),
            constants::DEFAULT_CONNECTIONS
        );
    }

    #[test]
    fn parse_connections_accepts_positive() {
        let mut options = BTreeMap::new();
        _ = options.insert("connections".into(), "3".into());
        assert_eq!(parse_connections(&options).unwrap(), 3);
    }

    #[test]
    fn parse_connections_rejects_zero() {
        let mut options = BTreeMap::new();
        _ = options.insert("connections".into(), "0".into());
        assert!(parse_connections(&options).is_err());
    }

    #[test]
    fn parse_message_timeout_defaults_to_twenty_seconds() {
        assert_eq!(
            parse_message_timeout(&BTreeMap::new()).unwrap(),
            Duration::from_secs(constants::DEFAULT_MESSAGE_TIMEOUT_SECS)
        );
    }

    #[test]
    fn parse_message_timeout_accepts_seconds() {
        let mut options = BTreeMap::new();
        _ = options.insert("message-timeout".into(), "45".into());
        assert_eq!(
            parse_message_timeout(&options).unwrap(),
            Duration::from_secs(45)
        );
    }

    #[test]
    fn parse_message_timeout_rejects_zero() {
        let mut options = BTreeMap::new();
        _ = options.insert("message-timeout".into(), "0".into());
        assert!(parse_message_timeout(&options).is_err());
    }

    #[test]
    fn parse_download_hedge_delay_supports_default_override_and_disable() {
        assert_eq!(parse_download_hedge_delay(&BTreeMap::new()).unwrap(), None);
        for (value, expected) in [("0", 0), ("2", 2)] {
            let options = BTreeMap::from([("download-hedge-delay".into(), value.into())]);
            assert_eq!(
                parse_download_hedge_delay(&options).unwrap(),
                Some(Duration::from_secs(expected))
            );
        }
        for value in ["-1", "abc", "1.5"] {
            let options = BTreeMap::from([("download-hedge-delay".into(), value.into())]);
            assert!(parse_download_hedge_delay(&options).is_err());
        }
    }

    #[test]
    fn object_key_layout() {
        let id: Id = "03dc1178e4e54f69beaf35dd9d4256a5a600e9fa3452b9db80bd649938923e67"
            .parse()
            .unwrap();
        let hex = id.to_hex();
        assert_eq!(object_key("", FileType::Config, &id), "config");
        assert_eq!(
            object_key("", FileType::Key, &id),
            format!("keys/{}", hex.as_str())
        );
        assert_eq!(
            object_key("home/", FileType::Pack, &id),
            format!("home/data/03/{}", hex.as_str())
        );
        assert_eq!(
            object_key("home/", FileType::Snapshot, &id),
            format!("home/snapshots/{}", hex.as_str())
        );
    }

    #[test]
    fn load_grant_from_access() {
        let mut options = BTreeMap::new();
        _ = options.insert("access".into(), "grant-bytes".into());
        assert_eq!(load_grant(&options).unwrap(), "grant-bytes");
    }

    #[test]
    fn load_grant_from_file() {
        let mut path = std::env::temp_dir();
        path.push("rustic-storj-grant-test.txt");
        {
            let mut f = std::fs::File::create(&path).unwrap();
            f.write_all(b"file-grant\n").unwrap();
        }
        let mut options = BTreeMap::new();
        _ = options.insert("access-file".into(), path.to_string_lossy().into_owned());
        let grant = load_grant(&options).unwrap();
        _ = std::fs::remove_file(&path);
        assert_eq!(grant.trim(), "file-grant");
    }

    #[test]
    fn load_grant_rejects_multiple_sources() {
        let mut options = BTreeMap::new();
        _ = options.insert("access".into(), "a".into());
        _ = options.insert("access-file".into(), "/tmp/x".into());
        assert!(load_grant(&options).is_err());
    }

    #[test]
    fn load_grant_missing() {
        if std::env::var_os("STORJ_ACCESS").is_some() {
            return;
        }
        let options = BTreeMap::new();
        assert!(load_grant(&options).is_err());
    }

    #[test]
    fn id_from_pack_key() {
        let id: Id = "03dc1178e4e54f69beaf35dd9d4256a5a600e9fa3452b9db80bd649938923e67"
            .parse()
            .unwrap();
        let key = format!("home/data/03/{}", id.to_hex().as_str());
        assert_eq!(id_from_object_key(&key, FileType::Pack), Some(id));
    }

    #[test]
    #[ignore = "needs STORJ_ACCESS and STORJ_BUCKET"]
    fn live_roundtrip() {
        let bucket = std::env::var("STORJ_BUCKET").expect("STORJ_BUCKET");
        let mut options = BTreeMap::new();
        _ = options.insert("retry".into(), "default".into());
        let be = StorjBackend::new(BackendLocation::from(bucket.as_str()), options)
            .expect("open storj backend");
        be.create().expect("ensure bucket");

        let id = Id::random();
        let payload = BytesList::from(b"rustic-storj-live-test".to_vec());
        be.write_bytes(FileType::Key, &id, false, payload)
            .expect("write");

        let full = be.read_full(FileType::Key, &id).expect("read_full");
        assert_eq!(&full[..], b"rustic-storj-live-test");

        let part = be
            .read_partial(FileType::Key, &id, false, 0, 6)
            .expect("read_partial");
        assert_eq!(&part[..], b"rustic");

        let listed = be.list_with_size(FileType::Key).expect("list");
        assert!(listed.iter().any(|(listed_id, _)| listed_id == &id));

        be.remove(FileType::Key, &id, false).expect("remove");
    }
}
