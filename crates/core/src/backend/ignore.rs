pub mod mapper;
pub use mapper::LocalSourceSaveOptions;

use std::{
    ffi::OsString,
    fs::File,
    io,
    path::{Path, PathBuf},
};

use bytesize::ByteSize;
use derive_setters::Setters;
use ignore::{Walk, WalkBuilder};
use log::{debug, warn};
use serde_with::{DisplayFromStr, serde_as};

#[cfg(not(windows))]
use std::num::TryFromIntError;

use crate::{
    Excludes,
    backend::{ReadSource, ReadSourceEntry, ReadSourceOpen},
    error::{ErrorKind, RusticError, RusticResult},
};

/// [`IgnoreErrorKind`] describes the errors that can be returned by a Ignore action in Backends
#[derive(thiserror::Error, Debug, displaydoc::Display)]
pub enum IgnoreErrorKind {
    #[cfg(all(not(windows), not(target_os = "openbsd")))]
    /// Error getting xattrs for `{path:?}`: `{source:?}`
    ErrorXattr {
        path: PathBuf,
        source: io::Error,
    },
    /// Error reading link target for `{path:?}`: `{source:?}`
    ErrorLink {
        path: PathBuf,
        source: io::Error,
    },
    #[cfg(not(windows))]
    /// Error converting ctime `{ctime}` and `ctime_nsec` `{ctime_nsec}` to Utc Timestamp: `{source:?}`
    CtimeConversionToTimestampFailed {
        ctime: i64,
        ctime_nsec: i64,
        source: TryFromIntError,
    },
    /// Error acquiring metadata for `{name}`: `{source:?}`
    AcquiringMetadataFailed { name: String, source: ignore::Error },
    /// time error
    JiffError(#[from] jiff::Error),
}

pub(crate) type IgnoreResult<T> = Result<T, IgnoreErrorKind>;

/// A [`LocalSource`] is a source from local paths which is used to be read from (i.e. to backup it).
#[derive(Debug)]
pub struct LocalSource {
    /// The walk builder.
    builder: WalkBuilder,
    /// The save options to use.
    save_opts: LocalSourceSaveOptions,
}

#[serde_as]
#[cfg_attr(feature = "clap", derive(clap::Parser))]
#[cfg_attr(feature = "merge", derive(conflate::Merge))]
#[derive(serde::Deserialize, serde::Serialize, Default, Clone, Debug, Setters)]
#[serde(default, rename_all = "kebab-case", deny_unknown_fields)]
#[setters(into)]
#[non_exhaustive]
/// [`LocalSourceFilterOptions`] allow to filter a local source by various criteria.
pub struct LocalSourceFilterOptions {
    /// Ignore files based on .gitignore files
    #[cfg_attr(feature = "clap", clap(long))]
    #[cfg_attr(feature = "merge", merge(strategy = conflate::bool::overwrite_false))]
    pub git_ignore: bool,

    /// Do not require a git repository to apply git-ignore rule
    #[cfg_attr(feature = "clap", clap(long))]
    #[cfg_attr(feature = "merge", merge(strategy = conflate::bool::overwrite_false))]
    pub no_require_git: bool,

    /// Treat the provided filename like a .gitignore file (can be specified multiple times)
    #[cfg_attr(
        feature = "clap",
        clap(long = "custom-ignorefile", value_name = "FILE")
    )]
    #[cfg_attr(feature = "merge", merge(strategy = conflate::vec::overwrite_empty))]
    pub custom_ignorefiles: Vec<String>,

    /// Exclude contents of directories containing this filename (can be specified multiple times)
    #[cfg_attr(feature = "clap", clap(long, value_name = "FILE"))]
    #[cfg_attr(feature = "merge", merge(strategy = conflate::vec::overwrite_empty))]
    pub exclude_if_present: Vec<String>,

    /// Exclude files/directories having the given extended attribute set (can be specified multiple times)
    #[cfg_attr(feature = "clap", clap(long, value_name = "XATTR"))]
    #[cfg_attr(feature = "merge", merge(strategy = conflate::vec::overwrite_empty))]
    pub exclude_if_xattr: Vec<String>,

    /// Exclude other file systems, don't cross filesystem boundaries and subvolumes
    #[cfg_attr(feature = "clap", clap(long, short = 'x'))]
    #[cfg_attr(feature = "merge", merge(strategy = conflate::bool::overwrite_false))]
    pub one_file_system: bool,

    /// Maximum size of files to be backed up. Larger files will be excluded.
    #[cfg_attr(feature = "clap", clap(long, value_name = "SIZE"))]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[cfg_attr(feature = "merge", merge(strategy = conflate::option::overwrite_none))]
    pub exclude_larger_than: Option<ByteSize>,
}

impl LocalSource {
    /// Create a local source from [`LocalSourceSaveOptions`], [`LocalSourceFilterOptions`] and backup path(s).
    ///
    /// # Arguments
    ///
    /// * `save_opts` - The [`LocalSourceSaveOptions`] to use.
    /// * `filter_opts` - The [`LocalSourceFilterOptions`] to use.
    /// * `backup_paths` - The backup path(s) to use.
    ///
    /// # Returns
    ///
    /// The created local source.
    ///
    /// # Errors
    ///
    /// * If the a glob pattern could not be added to the override builder.
    /// * If a glob file could not be read.
    #[allow(clippy::too_many_lines)]
    pub fn new(
        save_opts: LocalSourceSaveOptions,
        excludes: &Excludes,
        filter_opts: &LocalSourceFilterOptions,
        backup_paths: &[impl AsRef<Path>],
    ) -> RusticResult<Self> {
        let mut walk_builder = WalkBuilder::new(&backup_paths[0]);

        for path in &backup_paths[1..] {
            _ = walk_builder.add(path);
        }

        let overrides = excludes.as_override()?;

        for file in &filter_opts.custom_ignorefiles {
            _ = walk_builder.add_custom_ignore_filename(file);
        }

        _ = walk_builder
            .follow_links(false)
            .hidden(false)
            .ignore(false)
            .git_ignore(filter_opts.git_ignore)
            .git_exclude(filter_opts.git_ignore)
            .require_git(!filter_opts.no_require_git)
            .sort_by_file_path(Path::cmp)
            .same_file_system(filter_opts.one_file_system)
            .max_filesize(filter_opts.exclude_larger_than.map(|s| s.as_u64()))
            .overrides(overrides);

        let exclude_if_present = filter_opts.exclude_if_present.clone();
        let exclude_if_xattr: Vec<OsString> = filter_opts
            .exclude_if_xattr
            .iter()
            .map(OsString::from)
            .collect();

        if !exclude_if_xattr.is_empty() {
            #[cfg(any(windows, target_os = "openbsd"))]
            warn!("exclude-if-xattr is not supported on this platform");
            #[cfg(not(any(windows, target_os = "openbsd")))]
            if !xattr::SUPPORTED_PLATFORM {
                warn!("exclude-if-xattr is not supported on this platform");
            }
        }

        let needs_entry_filter = !exclude_if_present.is_empty() || !exclude_if_xattr.is_empty();

        if needs_entry_filter {
            _ = walk_builder.filter_entry(move |entry| {
                // exclude-if-present: skip directories containing a marker file
                if !exclude_if_present.is_empty()
                    && let Some(tpe) = entry.file_type()
                    && tpe.is_dir()
                    && exclude_if_present
                        .iter()
                        .any(|file| entry.path().join(file).exists())
                {
                    return false;
                }

                // exclude-if-xattr: skip entries that have a matching xattr
                #[cfg(not(any(windows, target_os = "openbsd")))]
                if xattr::SUPPORTED_PLATFORM && !exclude_if_xattr.is_empty() {
                    match xattr::list(entry.path()) {
                        Ok(mut attrs) => {
                            if attrs.any(|attr| exclude_if_xattr.contains(&attr)) {
                                return false;
                            }
                        }
                        Err(err) => {
                            warn!(
                                "Error reading xattrs for {}, not excluding: {err}",
                                entry.path().display()
                            );
                        }
                    }
                }

                true
            });
        }

        let builder = walk_builder;

        Ok(Self { builder, save_opts })
    }
}

#[derive(Debug)]
/// Describes an open file from the local backend.
pub struct OpenFile(PathBuf);

impl ReadSourceOpen for OpenFile {
    type Reader = File;

    /// Open the file from the local backend.
    ///
    /// # Returns
    ///
    /// The read handle to the file from the local backend.
    ///
    /// # Errors
    ///
    /// * If the file could not be opened.
    fn open(self) -> RusticResult<Self::Reader> {
        let path = self.0;
        File::open(&path).map_err(|err| {
            RusticError::with_source(
                ErrorKind::InputOutput,
                "Failed to open file at `{path}`. Please make sure the file exists and is accessible.",
                err,
            )
            .attach_context("path", path.display().to_string())
        })
    }
}

impl ReadSource for LocalSource {
    type Open = OpenFile;
    type Iter = LocalSourceWalker;

    /// Get the size of the local source.
    ///
    /// # Returns
    ///
    /// The size of the local source or `None` if the size could not be determined.
    ///
    /// # Errors
    ///
    /// * If the size could not be determined.
    fn size(&self) -> RusticResult<Option<u64>> {
        let mut size = 0;
        for entry in self.builder.build() {
            match entry.and_then(|e| e.metadata()) {
                Ok(m) => size += if m.is_dir() { 0 } else { m.len() },
                // Vanished paths during the parallel size scan are expected.
                // Don't warn: that log can freeze the backup progress bar.
                Err(err) if ignore_error_is_not_found(&err) => {
                    debug!("skipping vanished source path during size scan: {err}");
                }
                Err(err) => warn!("ignoring error {err}"),
            }
        }
        Ok(Some(size))
    }

    /// Iterate over the entries of the local source.
    ///
    /// # Returns
    ///
    /// An iterator over the entries of the local source.
    fn entries(&self) -> Self::Iter {
        LocalSourceWalker {
            walker: self.builder.build(),
            save_opts: self.save_opts,
        }
    }
}

fn ignore_error_path(err: &ignore::Error) -> Option<String> {
    match err {
        ignore::Error::WithPath { path, .. } => Some(path.display().to_string()),
        ignore::Error::WithDepth { err, .. } | ignore::Error::WithLineNumber { err, .. } => {
            ignore_error_path(err)
        }
        ignore::Error::Loop { child, .. } => Some(child.display().to_string()),
        _ => None,
    }
}

fn ignore_error_is_not_found(err: &ignore::Error) -> bool {
    err.io_error()
        .is_some_and(|io_err| io_err.kind() == io::ErrorKind::NotFound)
}

// Walk doesn't implement Debug
#[allow(missing_debug_implementations)]
pub struct LocalSourceWalker {
    /// The walk iterator.
    walker: Walk,
    /// The save options to use.
    save_opts: LocalSourceSaveOptions,
}

impl Iterator for LocalSourceWalker {
    type Item = RusticResult<ReadSourceEntry<OpenFile>>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.walker.next()? {
                // ignore root dir, i.e. an entry with depth 0 of type dir
                Ok(entry) if entry.depth() == 0 && entry.file_type().is_some_and(|t| t.is_dir()) => {
                    continue;
                }
                item => return Some(self.map_walk_item(item)),
            }
        }
    }
}

impl LocalSourceWalker {
    fn map_walk_item(
        &self,
        e: Result<ignore::DirEntry, ignore::Error>,
    ) -> RusticResult<ReadSourceEntry<OpenFile>> {
        let entry = e.map_err(|err| {
            let path = ignore_error_path(&err);
            let rustic_err = if path.is_some() {
                RusticError::with_source(
                    ErrorKind::InputOutput,
                    "Failed to read source path `{path}`.",
                    err,
                )
            } else {
                RusticError::with_source(
                    ErrorKind::InputOutput,
                    "Failed to read source path.",
                    err,
                )
            };
            match path {
                Some(path) => rustic_err.attach_context("path", path),
                None => rustic_err,
            }
        })?;
        let path = entry.path().display().to_string();
        self.save_opts.map_entry(entry).map_err(|err| {
            RusticError::with_source(
                ErrorKind::InputOutput,
                "Failed to map directory entry `{path}` to a backup source entry.",
                err,
            )
            .attach_context("path", path)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::tree::excludes::Excludes;
    use std::{
        fs,
        path::{Path, PathBuf},
    };

    fn source(base: &Path) -> LocalSource {
        LocalSource::new(
            LocalSourceSaveOptions::default(),
            &Excludes::default(),
            &LocalSourceFilterOptions::default(),
            &[base],
        )
        .unwrap()
    }

    #[test]
    fn ignore_io_not_found_is_detected() {
        let err = ignore::Error::Io(io::Error::from(io::ErrorKind::NotFound));
        assert!(ignore_error_is_not_found(&err));
        let wrapped = ignore::Error::WithPath {
            path: PathBuf::from("/tmp/__pycache__"),
            err: Box::new(err),
        };
        assert!(ignore_error_is_not_found(&wrapped));
        assert!(!ignore_error_is_not_found(&ignore::Error::Io(
            io::Error::from(io::ErrorKind::PermissionDenied)
        )));
    }

    #[test]
    fn size_scan_succeeds_after_directory_vanishes() {
        let tmp = tempfile::tempdir().unwrap();
        let base = tmp.path();
        fs::write(base.join("keep.txt"), "keep").unwrap();
        let vanished = base.join("__pycache__");
        fs::create_dir(&vanished).unwrap();
        fs::write(vanished.join("foo.pyc"), "pyc").unwrap();

        let src = source(base);
        fs::remove_dir_all(&vanished).unwrap();

        let size = src.size().unwrap().expect("size scan returns a total");
        assert_eq!(size, 4);
    }

    #[test]
    fn entries_walk_continues_after_directory_deleted_mid_iteration() {
        let tmp = tempfile::tempdir().unwrap();
        let base = tmp.path();
        // `0.txt` sorts before `__pycache__` so we can delete the dir after
        // the walk has started but before it descends into it.
        fs::write(base.join("0.txt"), "zero").unwrap();
        let vanished = base.join("__pycache__");
        fs::create_dir(&vanished).unwrap();
        fs::write(vanished.join("foo.pyc"), "pyc").unwrap();
        fs::write(base.join("keep.txt"), "keep").unwrap();

        let src = source(base);
        let mut iter = src.entries();
        let first = iter.next().expect("walker yields 0.txt");
        assert!(first.is_ok(), "{first:?}");
        fs::remove_dir_all(&vanished).unwrap();

        let rest: Vec<_> = iter.collect();
        assert!(
            rest.iter()
                .any(|item| item.as_ref().is_ok_and(|e| e.path.ends_with("keep.txt"))),
            "walk should continue after a vanished directory, got {rest:?}"
        );
    }
}
