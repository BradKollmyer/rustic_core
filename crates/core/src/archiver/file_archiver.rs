use std::io::Read;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::{
    archiver::{
        parent::{ItemWithParent, ParentResult},
        tree::TreeType,
        tree_archiver::TreeItem,
    },
    backend::{
        ReadSourceOpen,
        decrypt::DecryptWriteBackend,
        node::{Node, NodeType},
    },
    blob::{
        BlobId, BlobType, DataId,
        packer::{PackSizer, Packer, PackerStats},
        upload_pool::UploadSender,
    },
    chunker::ChunkIter,
    crypto::hasher::hash,
    error::{ErrorKind, RusticError, RusticResult},
    index::{ReadGlobalIndex, indexer::SharedIndexer},
    progress::Progress,
    repofile::configfile::ConfigFile,
};

/// Live new/changed file counts for the backup "uploading" progress line.
#[derive(Clone, Debug)]
pub(crate) struct UploadStats {
    p: Progress,
    files_new: Arc<AtomicU64>,
    files_changed: Arc<AtomicU64>,
    bytes: Arc<AtomicU64>,
}

impl UploadStats {
    pub(crate) fn new(p: Progress) -> Self {
        let stats = Self {
            p,
            files_new: Arc::new(AtomicU64::new(0)),
            files_changed: Arc::new(AtomicU64::new(0)),
            bytes: Arc::new(AtomicU64::new(0)),
        };
        stats.refresh();
        stats
    }

    fn note_file(&self, new: bool) {
        if new {
            _ = self.files_new.fetch_add(1, Ordering::Relaxed);
        } else {
            _ = self.files_changed.fetch_add(1, Ordering::Relaxed);
        }
        self.refresh();
    }

    fn add_bytes(&self, n: u64) {
        _ = self.bytes.fetch_add(n, Ordering::Relaxed);
        self.refresh();
    }

    fn refresh(&self) {
        self.p.set_upload_stats(
            self.files_new.load(Ordering::Relaxed),
            self.files_changed.load(Ordering::Relaxed),
            self.bytes.load(Ordering::Relaxed),
        );
    }

    pub(crate) fn finish(&self) {
        self.refresh();
        self.p.finish();
    }
}

/// The `FileArchiver` is responsible for archiving files.
/// It will read the file, chunk it, and write the chunks to the backend.
///
/// # Type Parameters
///
/// * `BE` - The backend type.
/// * `I` - The index to read from.
#[derive(Clone)]
pub(crate) struct FileArchiver<'a, BE: DecryptWriteBackend, I: ReadGlobalIndex> {
    index: &'a I,
    data_packer: Packer<BE>,
    config: ConfigFile,
}

impl<'a, BE: DecryptWriteBackend, I: ReadGlobalIndex> FileArchiver<'a, BE, I> {
    /// Creates a new `FileArchiver`.
    ///
    /// # Type Parameters
    ///
    /// * `BE` - The backend type.
    /// * `I` - The index to read from.
    ///
    /// # Arguments
    ///
    /// * `be` - The backend to write to.
    /// * `index` - The index to read from.
    /// * `indexer` - The indexer to write to.
    /// * `config` - The config file.
    ///
    /// # Errors
    ///
    /// * If sending the message to the raw packer fails.
    /// * If converting the data length to u64 fails
    pub(crate) fn new(
        be: BE,
        index: &'a I,
        indexer: SharedIndexer<BE>,
        config: &ConfigFile,
        uploads: Option<UploadSender>,
    ) -> RusticResult<Self> {
        let pack_sizer =
            PackSizer::from_config(config, BlobType::Data, index.total_size(BlobType::Data));
        let data_packer =
            Packer::new_with_uploads(be, BlobType::Data, indexer, pack_sizer, uploads)?;

        Ok(Self {
            index,
            data_packer,
            config: config.clone(),
        })
    }

    /// Processes the given item.
    ///
    /// # Type Parameters
    ///
    /// * `O` - The type of the tree item.
    ///
    /// # Arguments
    ///
    /// * `item` - The item to process.
    /// * `p` - The progress tracker.
    ///
    /// # Errors
    ///
    /// * If the item could not be unpacked.
    ///
    /// # Returns
    ///
    /// The processed item.
    pub(crate) fn process<O: ReadSourceOpen>(
        &self,
        item: ItemWithParent<Option<O>>,
        p: &Progress,
        upload: &UploadStats,
    ) -> RusticResult<TreeItem> {
        Ok(match item {
            TreeType::NewTree(item) => TreeType::NewTree(item),
            TreeType::EndTree => TreeType::EndTree,
            TreeType::Other((path, node, (open, parent))) => {
                let (node, filesize) = if matches!(parent, ParentResult::Matched(())) {
                    let size = node.meta.size;
                    p.inc(size);
                    (node, size)
                } else if node.node_type == NodeType::File {
                    let new = matches!(parent, ParentResult::NotFound);
                    let r = open
                        .ok_or_else(
                            || RusticError::new(
                                ErrorKind::Internal,
                                "Failed to unpack tree type optional at `{path}`. Option should contain a value, but contained `None`.",
                            )
                            .attach_context("path", path.display().to_string())
                            .ask_report(),
                        )?
                        .open()
                        .map_err(|err| {
                            err
                            .overwrite_kind(ErrorKind::InputOutput)
                            .prepend_guidance_line("Failed to open ReadSourceOpen at `{path}`")
                            .attach_context("path", path.display().to_string())
                        })?;

                    self.backup_reader(r, node, p, upload, new).map_err(|err| {
                        err.prepend_guidance_line("Error while backing up `{path}`")
                            .attach_context("path", path.display().to_string())
                    })?
                } else {
                    (node, 0)
                };
                TreeType::Other((path, node, (parent, filesize)))
            }
        })
    }

    // TODO: add documentation!
    fn backup_reader(
        &self,
        r: impl Read + Send + 'static,
        node: Node,
        p: &Progress,
        upload: &UploadStats,
        new: bool,
    ) -> RusticResult<(Node, u64)> {
        upload.note_file(new);
        let chunks: Vec<_> = ChunkIter::from_config(
            &self.config,
            r,
            usize::try_from(node.meta.size).unwrap_or(usize::MAX),
        )?
        .map(|chunk| {
            let chunk = chunk?;
            let id = hash(&chunk);
            let size = chunk.len() as u64;

            if !self.index.has_data(&DataId::from(id)) {
                self.data_packer.add(chunk.into(), BlobId::from(id))?;
            }
            p.inc(size);
            upload.add_bytes(size);
            Ok((DataId::from(id), size))
        })
        .collect::<RusticResult<_>>()?;

        let filesize = chunks.iter().map(|x| x.1).sum();
        let content = chunks.into_iter().map(|x| x.0).collect();

        let mut node = node;
        node.content = Some(content);
        Ok((node, filesize))
    }

    /// Finalizes the archiver.
    ///
    /// # Returns
    ///
    /// The statistics of the archiver.
    ///
    /// # Panics
    ///
    /// * If the channel could not be dropped
    pub(crate) fn finalize(self) -> RusticResult<PackerStats> {
        self.data_packer.finalize()
    }
}
