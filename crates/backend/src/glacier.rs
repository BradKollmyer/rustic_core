//! Rustic-only Glacier / S3 archive options parsed from the OpenDAL option map.

use std::collections::BTreeMap;

use log::warn;
use rustic_core::{ErrorKind, RusticError, RusticResult};

const ARCHIVE_CLASSES: &[&str] = &["GLACIER", "DEEP_ARCHIVE", "GLACIER_IR"];

/// Keys rustic consumes and must strip before `Operator::via_iter`.
const RUSTIC_KEYS: &[&str] = &[
    "data_storage_class",
    "enable_restore",
    "restore_days",
    "restore_tier",
    "restore_timeout",
];

/// Glacier-related options stored on [`crate::opendal::OpenDALBackend`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct GlacierConfig {
    /// Storage class applied only to non-cacheable (data) packs.
    pub data_storage_class: Option<String>,
    /// Opt-in native RestoreObject.
    pub enable_restore: bool,
    /// RestoreObject `Days`.
    pub restore_days: i32,
    /// RestoreObject tier: `Expedited`, `Standard`, or `Bulk`.
    pub restore_tier: String,
    /// Poll deadline, e.g. `"48h"`.
    pub restore_timeout: Option<String>,
}

impl GlacierConfig {
    /// Remove rustic-only keys from `options` and return the parsed config.
    pub(crate) fn extract(options: &mut BTreeMap<String, String>) -> RusticResult<Self> {
        let data_storage_class = options.remove("data_storage_class");
        let enable_restore = parse_bool(options.remove("enable_restore").as_deref())?;
        let restore_days = parse_days(options.remove("restore_days").as_deref())?;
        let restore_tier = options
            .remove("restore_tier")
            .unwrap_or_else(|| "Standard".to_string());
        let restore_timeout = options.remove("restore_timeout");

        if let Some(class) = &data_storage_class {
            let upper = class.to_ascii_uppercase();
            if !ARCHIVE_CLASSES.contains(&upper.as_str())
                && upper != "STANDARD"
                && upper != "STANDARD_IA"
                && upper != "ONEZONE_IA"
                && upper != "INTELLIGENT_TIERING"
                && upper != "REDUCED_REDUNDANCY"
            {
                warn_unknown_class(class);
            }
        }

        let _ = RUSTIC_KEYS;
        Ok(Self {
            data_storage_class,
            enable_restore,
            restore_days,
            restore_tier,
            restore_timeout,
        })
    }

    pub(crate) fn archive_class(&self) -> Option<&str> {
        self.data_storage_class
            .as_deref()
            .filter(|class| ARCHIVE_CLASSES.contains(&class.to_ascii_uppercase().as_str()))
    }

    pub(crate) fn is_instant_retrieval(&self) -> bool {
        self.data_storage_class
            .as_deref()
            .is_some_and(|c| c.eq_ignore_ascii_case("GLACIER_IR"))
    }
}

fn parse_bool(value: Option<&str>) -> RusticResult<bool> {
    match value {
        None => Ok(false),
        Some("true" | "1" | "yes") => Ok(true),
        Some("false" | "0" | "no") => Ok(false),
        Some(other) => Err(RusticError::new(
            ErrorKind::InvalidInput,
            "enable_restore must be \"true\" or \"false\", got `{value}`.",
        )
        .attach_context("value", other.to_string())),
    }
}

fn parse_days(value: Option<&str>) -> RusticResult<i32> {
    match value {
        None => Ok(7),
        Some(s) => s.parse().map_err(|err| {
            RusticError::with_source(
                ErrorKind::InvalidInput,
                "restore_days `{value}` is not an integer.",
                err,
            )
            .attach_context("value", s.to_string())
        }),
    }
}

fn warn_unknown_class(class: &str) {
    warn!("unknown data_storage_class `{class}`; applying it to data packs anyway");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_strips_rustic_keys_and_keeps_opendal_keys() {
        let mut opts = BTreeMap::from([
            ("bucket".into(), "b".into()),
            ("data_storage_class".into(), "DEEP_ARCHIVE".into()),
            ("enable_restore".into(), "true".into()),
            ("restore_days".into(), "3".into()),
            ("restore_tier".into(), "Bulk".into()),
            ("restore_timeout".into(), "48h".into()),
            ("retry".into(), "5".into()),
        ]);
        let cfg = GlacierConfig::extract(&mut opts).unwrap();
        assert_eq!(cfg.data_storage_class.as_deref(), Some("DEEP_ARCHIVE"));
        assert!(cfg.enable_restore);
        assert_eq!(cfg.restore_days, 3);
        assert_eq!(cfg.restore_tier, "Bulk");
        assert_eq!(cfg.restore_timeout.as_deref(), Some("48h"));
        assert_eq!(opts.get("bucket").map(String::as_str), Some("b"));
        assert_eq!(opts.get("retry").map(String::as_str), Some("5"));
        assert!(!opts.contains_key("data_storage_class"));
        assert!(!opts.contains_key("enable_restore"));
        assert_eq!(cfg.archive_class(), Some("DEEP_ARCHIVE"));
        assert!(!cfg.is_instant_retrieval());
    }

    #[test]
    fn glacier_ir_is_instant_and_archive() {
        let mut opts = BTreeMap::from([("data_storage_class".into(), "GLACIER_IR".into())]);
        let cfg = GlacierConfig::extract(&mut opts).unwrap();
        assert_eq!(cfg.archive_class(), Some("GLACIER_IR"));
        assert!(cfg.is_instant_retrieval());
    }
}
