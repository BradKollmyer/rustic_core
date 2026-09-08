//! Local fault and latency tests for the opt-in coordinated prune pipeline.
#![allow(unused_results)]
use anyhow::Result;
use bytes::Bytes;
use bytesize::ByteSize;
use rustic_backend::LocalBackend;
use rustic_core::{
    BackupOptions, BytesList, CheckOptions, ConfigOptions, Credentials, ErrorKind, FileType, Id,
    KeyOptions, LimitOption, OpenStatus, PathList, PruneOptions, ReadBackend, Repository,
    RepositoryBackends, RepositoryOptions, RusticError, RusticResult, WriteBackend,
    repofile::{Chunker, IndexFile, MasterKey, SnapshotFile},
};
use std::{
    collections::BTreeSet,
    fs,
    path::Path,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::SeqCst},
    },
    thread,
    time::{Duration, Instant},
};
use tempfile::{TempDir, tempdir};

#[derive(Default)]
struct Metrics {
    enabled: AtomicBool,
    connection_limit: AtomicUsize,
    reads: AtomicUsize,
    writes: AtomicUsize,
    peak_reads: AtomicUsize,
    peak_writes: AtomicUsize,
    active: AtomicUsize,
    peak: AtomicUsize,
    overlap: AtomicBool,
    gets: AtomicUsize,
    puts: AtomicUsize,
    deletes: AtomicUsize,
    read_bytes: AtomicU64,
    write_bytes: AtomicU64,
    live_read_bytes: AtomicU64,
    peak_read_bytes: AtomicU64,
    live_write_bytes: AtomicU64,
    peak_write_bytes: AtomicU64,
    read_ms: AtomicU64,
    write_ms: AtomicU64,
    read_mib_per_second: AtomicU64,
    write_mib_per_second: AtomicU64,
    failures: AtomicUsize,
    fail_read_at: AtomicUsize,
    index_puts: AtomicUsize,
    index_active: AtomicUsize,
    index_peak: AtomicUsize,
    index_bytes: AtomicU64,
    index_delay_ms: AtomicU64,
    repack_index_delay_ms: AtomicU64,
    repack_index_puts: AtomicUsize,
    repack_index_peak: AtomicUsize,
    index_failures: AtomicUsize,
    repack_index_failures: AtomicUsize,
    stall_only_repack_indexes: AtomicBool,
    index_stall: Mutex<bool>,
    index_wake: Condvar,
    stall: Mutex<bool>,
    wake: Condvar,
    completed: Mutex<BTreeSet<Id>>,
    published: Mutex<Vec<(Id, BTreeSet<Id>)>>,
}
struct IndexActive<'a>(&'a Metrics);
impl Drop for IndexActive<'_> {
    fn drop(&mut self) {
        self.0.index_active.fetch_sub(1, SeqCst);
        self.0.active.fetch_sub(1, SeqCst);
    }
}
struct HeldBytes {
    metrics: Arc<Metrics>,
    bytes: u64,
    upload: bool,
}
impl HeldBytes {
    fn new(metrics: &Arc<Metrics>, bytes: u64, upload: bool) -> Self {
        let (live, peak) = if upload {
            (&metrics.live_write_bytes, &metrics.peak_write_bytes)
        } else {
            (&metrics.live_read_bytes, &metrics.peak_read_bytes)
        };
        peak.fetch_max(live.fetch_add(bytes, SeqCst) + bytes, SeqCst);
        Self {
            metrics: metrics.clone(),
            bytes,
            upload,
        }
    }
}
impl Drop for HeldBytes {
    fn drop(&mut self) {
        let live = if self.upload {
            &self.metrics.live_write_bytes
        } else {
            &self.metrics.live_read_bytes
        };
        live.fetch_sub(self.bytes, SeqCst);
    }
}
struct TrackedRead {
    data: Bytes,
    _hold: HeldBytes,
}
impl AsRef<[u8]> for TrackedRead {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

struct Active<'a> {
    metrics: &'a Metrics,
    upload: bool,
}
impl Metrics {
    fn enter(&self, upload: bool) -> Active<'_> {
        let (active, peak) = if upload {
            (&self.writes, &self.peak_writes)
        } else {
            (&self.reads, &self.peak_reads)
        };
        peak.fetch_max(active.fetch_add(1, SeqCst) + 1, SeqCst);
        self.peak
            .fetch_max(self.active.fetch_add(1, SeqCst) + 1, SeqCst);
        if self.reads.load(SeqCst) > 0 && self.writes.load(SeqCst) > 0 {
            self.overlap.store(true, SeqCst);
        }
        Active {
            metrics: self,
            upload,
        }
    }
}
impl Drop for Active<'_> {
    fn drop(&mut self) {
        let count = if self.upload {
            &self.metrics.writes
        } else {
            &self.metrics.reads
        };
        count.fetch_sub(1, SeqCst);
        self.metrics.active.fetch_sub(1, SeqCst);
    }
}
#[allow(clippy::cast_precision_loss)]
fn transfer_delay(milliseconds: u64, bytes: u64, mib_per_second: u64) -> Duration {
    Duration::from_millis(milliseconds)
        + if mib_per_second == 0 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64(bytes as f64 / (mib_per_second as f64 * 1_048_576.0))
        }
}

#[derive(Clone)]
struct DelayedBackend {
    inner: LocalBackend,
    metrics: Arc<Metrics>,
}
impl ReadBackend for DelayedBackend {
    fn connection_limit(&self) -> Option<usize> {
        match self.metrics.connection_limit.load(SeqCst) {
            0 => None,
            n => Some(n),
        }
    }

    fn location(&self) -> String {
        self.inner.location()
    }
    fn list_with_size(&self, t: FileType) -> RusticResult<Vec<(Id, u32)>> {
        self.inner.list_with_size(t)
    }
    fn read_full(&self, t: FileType, id: &Id) -> RusticResult<Bytes> {
        self.inner.read_full(t, id)
    }
    fn warmup_path(&self, t: FileType, id: &Id) -> String {
        self.inner.warmup_path(t, id)
    }
    fn read_partial(&self, t: FileType, id: &Id, c: bool, o: u32, l: u32) -> RusticResult<Bytes> {
        if !self.metrics.enabled.load(SeqCst) {
            return self.inner.read_partial(t, id, c, o, l);
        }
        let _active = self.metrics.enter(false);
        let request = self.metrics.gets.fetch_add(1, SeqCst) + 1;
        if request == 1 {
            assert_eq!(
                self.metrics.index_active.load(SeqCst),
                0,
                "repacking started before index rebuild uploads finished"
            );
        }
        if self.metrics.fail_read_at.load(SeqCst) == request {
            return Err(RusticError::new(
                ErrorKind::Backend,
                "injected pack read failure",
            ));
        }
        self.metrics.read_bytes.fetch_add(u64::from(l), SeqCst);
        thread::sleep(transfer_delay(
            self.metrics.read_ms.load(SeqCst),
            u64::from(l),
            self.metrics.read_mib_per_second.load(SeqCst),
        ));
        let hold = HeldBytes::new(&self.metrics, u64::from(l), false);
        let data = self.inner.read_partial(t, id, c, o, l)?;
        Ok(Bytes::from_owner(TrackedRead { data, _hold: hold }))
    }
}
impl WriteBackend for DelayedBackend {
    fn create(&self) -> RusticResult<()> {
        self.inner.create()
    }
    fn remove(&self, t: FileType, id: &Id, c: bool) -> RusticResult<()> {
        if self.metrics.enabled.load(SeqCst) {
            assert_eq!(
                self.metrics.index_active.load(SeqCst),
                0,
                "deletion raced an index upload"
            );
            self.metrics.deletes.fetch_add(1, SeqCst);
        }
        self.inner.remove(t, id, c)
    }
    #[allow(clippy::significant_drop_tightening)]
    fn write_bytes(&self, t: FileType, id: &Id, c: bool, data: BytesList) -> RusticResult<()> {
        if !self.metrics.enabled.load(SeqCst) {
            return self.inner.write_bytes(t, id, c, data);
        }
        if t == FileType::Pack {
            let _active = self.metrics.enter(true);
            let _hold = HeldBytes::new(&self.metrics, data.size() as u64, true);
            self.metrics.puts.fetch_add(1, SeqCst);
            self.metrics.write_bytes.fetch_add(
                data.slice().iter().map(|b| b.len() as u64).sum::<u64>(),
                SeqCst,
            );
            let stalled = self.metrics.stall.lock().unwrap();
            let (stalled, _) = self
                .metrics
                .wake
                .wait_timeout_while(stalled, Duration::from_secs(5), |value| *value)
                .unwrap();
            if *stalled {
                return Err(RusticError::new(
                    ErrorKind::Backend,
                    "test upload stall deadline",
                ));
            }
            drop(stalled);
            thread::sleep(transfer_delay(
                self.metrics.write_ms.load(SeqCst),
                data.size() as u64,
                self.metrics.write_mib_per_second.load(SeqCst),
            ));
            if self
                .metrics
                .failures
                .fetch_update(SeqCst, SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                return Err(
                    RusticError::new(ErrorKind::Backend, "injected pack upload failure")
                        .attach_context("failed_pack", id.to_string()),
                );
            }
            self.inner.write_bytes(t, id, c, data)?;
            self.metrics.completed.lock().unwrap().insert(*id);
            Ok(())
        } else if t == FileType::Index {
            let m = &self.metrics;
            m.index_peak
                .fetch_max(m.index_active.fetch_add(1, SeqCst) + 1, SeqCst);
            m.peak.fetch_max(m.active.fetch_add(1, SeqCst) + 1, SeqCst);
            let _active = IndexActive(m);
            m.index_puts.fetch_add(1, SeqCst);
            let repacking = m.gets.load(SeqCst) > 0;
            if repacking {
                m.repack_index_puts.fetch_add(1, SeqCst);
                m.repack_index_peak
                    .fetch_max(m.index_active.load(SeqCst), SeqCst);
            }
            m.index_bytes.fetch_add(data.size() as u64, SeqCst);
            let stalled = m.index_stall.lock().unwrap();
            let (stalled, _) = m
                .index_wake
                .wait_timeout_while(stalled, Duration::from_secs(10), |value| {
                    *value && (!m.stall_only_repack_indexes.load(SeqCst) || repacking)
                })
                .unwrap();
            if *stalled && (!m.stall_only_repack_indexes.load(SeqCst) || repacking) {
                return Err(RusticError::new(
                    ErrorKind::Backend,
                    "test index stall deadline",
                ));
            }
            drop(stalled);
            thread::sleep(Duration::from_millis(if repacking {
                m.repack_index_delay_ms.load(SeqCst)
            } else {
                m.index_delay_ms.load(SeqCst)
            }));
            let failures = if repacking {
                &m.repack_index_failures
            } else {
                &m.index_failures
            };
            if failures
                .fetch_update(SeqCst, SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                return Err(
                    RusticError::new(ErrorKind::Backend, "injected index upload failure")
                        .attach_context("failed_index", id.to_string()),
                );
            }
            m.published
                .lock()
                .unwrap()
                .push((*id, m.completed.lock().unwrap().clone()));
            self.inner.write_bytes(t, id, c, data)
        } else {
            self.inner.write_bytes(t, id, c, data)
        }
    }
}

struct Seed {
    dir: TempDir,
    key: MasterKey,
}
fn seed(mebibytes: usize) -> Result<Seed> {
    seed_layout(mebibytes, 1, 64)
}

fn seed_layout(mebibytes: usize, pack_mib: u64, chunk_kib: u64) -> Result<Seed> {
    seed_layout_bytes(
        mebibytes,
        pack_mib,
        usize::try_from(chunk_kib * 1024)?,
        usize::try_from(chunk_kib * 1024)?,
        false,
    )
}

fn seed_layout_bytes(
    mebibytes: usize,
    pack_mib: u64,
    chunk_bytes: usize,
    file_bytes: usize,
    keep_only_marker: bool,
) -> Result<Seed> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    fs::create_dir(&source)?;
    let key = MasterKey::new();
    let backend = LocalBackend::new(dir.path().join("repo").to_str().unwrap(), None)?;
    let backends = RepositoryBackends::new(Arc::new(backend), None);
    let config = ConfigOptions::default()
        .set_chunker(Chunker::FixedSize)
        .set_chunk_size(ByteSize::b(chunk_bytes as u64))
        .set_datapack_size(ByteSize::mib(pack_mib))
        .set_datapack_growfactor(0)
        .set_treepack_growfactor(0)
        .set_compression(1);
    let repo = Repository::new(&RepositoryOptions::default().no_cache(true), &backends)?
        .init(
            &Credentials::Masterkey(key.clone()),
            &KeyOptions::default(),
            &config,
        )?
        .to_indexed_ids()?;
    let mut random = 0x0123_4567_89ab_cdef_u64;
    let file_count = mebibytes * 1024 * 1024 / file_bytes;
    for file in 0..file_count {
        let mut data = vec![0u8; file_bytes];
        for block in data.chunks_exact_mut(8) {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            block.copy_from_slice(&random.to_le_bytes());
        }
        fs::write(source.join(format!("{file:06}")), data)?;
    }
    let paths = PathList::from_iter([source.clone()]);
    let first = repo.backup(&BackupOptions::default(), &paths, SnapshotFile::default())?;
    for file in (0..file_count).step_by(if keep_only_marker { 1 } else { 2 }) {
        fs::remove_file(source.join(format!("{file:06}")))?;
    }
    if keep_only_marker {
        fs::write(source.join("retained"), vec![42u8; 1024])?;
    }
    let repo = repo.to_indexed_ids()?;
    repo.backup(&BackupOptions::default(), &paths, SnapshotFile::default())?;
    repo.drop_index().delete_snapshots(&[first.id])?;
    Ok(Seed { dir, key })
}
fn copy_dir(src: &Path, dst: &Path) -> Result<()> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            copy_dir(&entry.path(), &dst.join(entry.file_name()))?;
        } else {
            fs::copy(entry.path(), dst.join(entry.file_name()))?;
        }
    }
    Ok(())
}
fn trial(seed: &Seed) -> Result<(TempDir, Repository<OpenStatus>, DelayedBackend)> {
    let dir = tempdir()?;
    copy_dir(&seed.dir.path().join("repo"), dir.path())?;
    let metrics = Arc::new(Metrics::default());
    let backend = DelayedBackend {
        inner: LocalBackend::new(dir.path().to_str().unwrap(), None)?,
        metrics,
    };
    *backend.metrics.completed.lock().unwrap() =
        backend.list(FileType::Pack)?.into_iter().collect();
    let backends = RepositoryBackends::new(Arc::new(backend.clone()), None);
    let repo = Repository::new(&RepositoryOptions::default().no_cache(true), &backends)?
        .open(&Credentials::Masterkey(seed.key.clone()))?;
    Ok((dir, repo, backend))
}
fn opts(n: Option<usize>, fast: bool) -> PruneOptions {
    PruneOptions::default()
        .repack_connections(n)
        .fast_repack(fast)
        .instant_delete(true)
        .max_repack(LimitOption::Unlimited)
        .max_unused(LimitOption::Percentage(0))
}
fn published_after_upload(repo: &Repository<OpenStatus>, metrics: &Metrics) -> Result<()> {
    for (id, completed) in metrics.published.lock().unwrap().iter() {
        let index: IndexFile =
            serde_json::from_slice(&repo.cat_file(FileType::Index, &id.to_string())?)?;
        for pack in index.packs {
            assert!(
                completed.contains(&*pack.id),
                "index published before pack {} completed",
                pack.id
            );
        }
    }
    Ok(())
}

#[test]
fn backend_limits_reach_prune_through_cache_and_cap_overrides() -> Result<()> {
    let seed = seed(32)?;
    for (backend_limit, requested, expected) in [(5, None, 5), (3, Some(10), 3), (10, Some(3), 3)] {
        let (_dir, _repo, backend) = trial(&seed)?;
        let cache = tempdir()?;
        let backends = RepositoryBackends::new(Arc::new(backend.clone()), None);
        let repo = Repository::new(
            &RepositoryOptions::default().cache_dir(cache.path()),
            &backends,
        )?
        .open(&Credentials::Masterkey(seed.key.clone()))?;
        let m = &backend.metrics;
        m.connection_limit.store(backend_limit, SeqCst);
        assert!(backend.tree_loader_count() <= backend_limit);
        let opts = opts(requested, true).parallel_repack(true);
        let plan = repo.prune_plan(&opts)?;
        m.read_ms.store(15, SeqCst);
        m.write_ms.store(35, SeqCst);
        m.enabled.store(true, SeqCst);
        repo.prune(&opts, plan)?;
        assert!(m.peak.load(SeqCst) <= expected);
        assert!(m.peak_reads.load(SeqCst) < expected);
        assert!(m.peak_writes.load(SeqCst) > 1);
        published_after_upload(&repo, m)?;
        m.enabled.store(false, SeqCst);
        repo.check(CheckOptions::default().read_data(true))?
            .is_ok()?;
    }
    let (_dir, repo, backend) = trial(&seed)?;
    backend.metrics.connection_limit.store(1, SeqCst);
    let opts = opts(None, true).parallel_repack(true);
    let plan = repo.prune_plan(&opts)?;
    backend.metrics.enabled.store(true, SeqCst);
    assert!(repo.prune(&opts, plan).is_err());
    assert_eq!(backend.metrics.puts.load(SeqCst), 0);
    assert_eq!(backend.metrics.deletes.load(SeqCst), 0);
    Ok(())
}

#[test]
fn byte_limits_bound_live_buffers_and_split_large_ranges() -> Result<()> {
    let seed = seed(32)?;
    for fast in [false, true] {
        let (_dir, repo, backend) = trial(&seed)?;
        let m = &backend.metrics;
        let opts = opts(Some(10), fast)
            .repack_read_buffer(ByteSize::kib(256))
            .repack_upload_buffer(ByteSize::mib(2));
        let plan = repo.prune_plan(&opts)?;
        m.write_ms.store(10, SeqCst);
        m.enabled.store(true, SeqCst);
        repo.prune(&opts, plan)?;
        assert!(m.peak_read_bytes.load(SeqCst) <= opts.repack_read_buffer.as_u64());
        assert!(m.peak_write_bytes.load(SeqCst) <= opts.repack_upload_buffer.as_u64());
        assert!(
            m.gets.load(SeqCst) > 32,
            "ranges should split to fit the byte budget"
        );
        assert_eq!(m.live_read_bytes.load(SeqCst), 0);
        assert_eq!(m.live_write_bytes.load(SeqCst), 0);
        published_after_upload(&repo, m)?;
        m.enabled.store(false, SeqCst);
        repo.check(CheckOptions::default().read_data(true))?
            .is_ok()?;
    }
    Ok(())
}

#[test]
fn oversized_buffers_fail_with_sizing_context_without_deleting_old_data() -> Result<()> {
    let seed = seed(8)?;
    for (read, upload, option) in [
        (1, 2_097_152, "--repack-read-buffer"),
        (262_144, 1, "--repack-upload-buffer"),
    ] {
        let (_dir, repo, backend) = trial(&seed)?;
        let opts = opts(Some(5), true)
            .repack_read_buffer(ByteSize::b(read))
            .repack_upload_buffer(ByteSize::b(upload));
        let plan = repo.prune_plan(&opts)?;
        backend.metrics.enabled.store(true, SeqCst);
        let error = repo.prune(&opts, plan).unwrap_err();
        assert_eq!(error.context_value("option"), Some(option), "{error}");
        assert!(error.context_value("required_bytes").is_some());
        assert_eq!(backend.metrics.deletes.load(SeqCst), 0);
        assert_eq!(backend.metrics.active.load(SeqCst), 0);
        backend.metrics.enabled.store(false, SeqCst);
        repo.check(CheckOptions::default().read_data(true))?
            .is_ok()?;
    }
    for (read, upload) in [(0, 1), (1, 0)] {
        let (_dir, repo, backend) = trial(&seed)?;
        let opts = opts(Some(5), true)
            .repack_read_buffer(ByteSize::b(read))
            .repack_upload_buffer(ByteSize::b(upload));
        let plan = repo.prune_plan(&opts)?;
        backend.metrics.enabled.store(true, SeqCst);
        assert!(repo.prune(&opts, plan).is_err());
        assert_eq!(backend.metrics.puts.load(SeqCst), 0);
        assert_eq!(backend.metrics.deletes.load(SeqCst), 0);
    }
    Ok(())
}

#[test]
fn coordinated_prune_limits_and_integrity() -> Result<()> {
    let seed = seed(32)?;
    for fast in [false, true] {
        for n in [2, 5, 10] {
            let (_dir, repo, backend) = trial(&seed)?;
            let m = &backend.metrics;
            let opts = opts(Some(n), fast);
            let plan = repo.prune_plan(&opts)?;
            m.read_ms.store(15, SeqCst);
            m.write_ms.store(35, SeqCst);
            m.enabled.store(true, SeqCst);
            repo.prune(&opts, plan)?;
            assert!(m.peak.load(SeqCst) <= n);
            assert!(m.peak_reads.load(SeqCst) < n);
            assert!(m.peak_writes.load(SeqCst) <= n);
            assert!(m.peak_writes.load(SeqCst) > 1);
            assert!(m.overlap.load(SeqCst));
            assert_eq!(m.active.load(SeqCst), 0);
            published_after_upload(&repo, m)?;
            m.enabled.store(false, SeqCst);
            repo.check(CheckOptions::default().read_data(true))?
                .is_ok()?;
        }
    }
    Ok(())
}

#[test]
fn upload_failure_preserves_old_packs_and_indexes_and_joins_workers() -> Result<()> {
    let seed = seed(32)?;
    for fast in [false, true] {
        let (_dir, repo, backend) = trial(&seed)?;
        let m = &backend.metrics;
        let old_packs: BTreeSet<_> = backend.list(FileType::Pack)?.into_iter().collect();
        let old_indexes: BTreeSet<_> = backend.list(FileType::Index)?.into_iter().collect();
        let opts = opts(Some(5), fast);
        let plan = repo.prune_plan(&opts)?;
        m.failures.store(3, SeqCst);
        m.write_ms.store(30, SeqCst);
        m.enabled.store(true, SeqCst);
        let error = repo.prune(&opts, plan).unwrap_err();
        assert!(
            error.to_string().contains("injected pack upload failure"),
            "{error}"
        );
        let failures = 3 - m.failures.load(SeqCst);
        assert_eq!(
            error.context_value("upload_failures"),
            Some(failures.to_string().as_str())
        );
        assert!(error.context_value("pack_id").is_some());
        assert!(
            error
                .context_value("upload_errors")
                .unwrap()
                .contains("injected pack upload failure")
        );
        assert_eq!(m.deletes.load(SeqCst), 0);
        assert_eq!(m.active.load(SeqCst), 0);
        assert!(old_packs.is_subset(&backend.list(FileType::Pack)?.into_iter().collect()));
        assert!(old_indexes.is_subset(&backend.list(FileType::Index)?.into_iter().collect()));
        let completed = m.puts.load(SeqCst);
        thread::sleep(Duration::from_millis(100));
        assert_eq!(
            m.puts.load(SeqCst),
            completed,
            "upload work survived prune return"
        );
        published_after_upload(&repo, m)?;
        m.enabled.store(false, SeqCst);
        repo.check(CheckOptions::default().read_data(true))?
            .is_ok()?;
    }
    Ok(())
}

#[test]
fn parallel_index_rebuild_respects_backend_limit_and_deletion_barrier() -> Result<()> {
    let seed = seed_layout_bytes(16, 1, 64, 1024 * 1024, true)?;
    let (_dir, repo, backend) = trial(&seed)?;
    let m = &backend.metrics;
    m.connection_limit.store(3, SeqCst);
    let opts = opts(Some(10), false)
        .instant_delete(false)
        .max_repack("0".parse::<LimitOption>()?);
    let plan = repo.prune_plan(&opts)?;
    m.index_delay_ms.store(100, SeqCst);
    m.enabled.store(true, SeqCst);
    repo.prune(&opts, plan)?;
    assert!(m.index_puts.load(SeqCst) >= 3);
    assert!(m.index_peak.load(SeqCst) > 1);
    assert!(m.index_peak.load(SeqCst) <= 3);
    assert_eq!(m.index_active.load(SeqCst), 0);
    assert!(m.deletes.load(SeqCst) > 0);
    assert_eq!(m.puts.load(SeqCst), 0);
    published_after_upload(&repo, m)?;
    m.enabled.store(false, SeqCst);
    repo.check(CheckOptions::default().read_data(true))?
        .is_ok()?;
    Ok(())
}

#[test]
fn index_upload_failure_preserves_old_indexes_and_packs_and_joins_workers() -> Result<()> {
    let seed = seed_layout_bytes(16, 1, 64, 1024 * 1024, true)?;
    let (_dir, repo, backend) = trial(&seed)?;
    let m = &backend.metrics;
    let old_packs: BTreeSet<_> = backend.list(FileType::Pack)?.into_iter().collect();
    let old_indexes: BTreeSet<_> = backend.list(FileType::Index)?.into_iter().collect();
    let opts = opts(Some(5), false)
        .instant_delete(false)
        .max_repack("0".parse::<LimitOption>()?);
    let plan = repo.prune_plan(&opts)?;
    m.index_delay_ms.store(100, SeqCst);
    m.index_failures.store(2, SeqCst);
    m.enabled.store(true, SeqCst);
    let error = repo.prune(&opts, plan).unwrap_err();
    assert!(
        error.to_string().contains("injected index upload failure"),
        "{error}"
    );
    let failures = 2 - m.index_failures.load(SeqCst);
    assert_eq!(
        error.context_value("index_upload_failures"),
        Some(failures.to_string().as_str())
    );
    assert!(error.context_value("failed_index").is_some());
    assert!(
        error
            .context_value("index_upload_errors")
            .unwrap()
            .contains("injected index upload failure")
    );
    assert_eq!(m.deletes.load(SeqCst), 0);
    assert_eq!(m.puts.load(SeqCst), 0);
    assert_eq!(m.index_active.load(SeqCst), 0);
    assert!(old_indexes.is_subset(&backend.list(FileType::Index)?.into_iter().collect()));
    assert!(old_packs.is_subset(&backend.list(FileType::Pack)?.into_iter().collect()));
    let completed = m.index_puts.load(SeqCst);
    thread::sleep(Duration::from_millis(100));
    assert_eq!(m.index_puts.load(SeqCst), completed);
    m.enabled.store(false, SeqCst);
    repo.check(CheckOptions::default().read_data(true))?
        .is_ok()?;
    Ok(())
}

#[test]
fn stalled_repack_indexes_release_lock_and_respect_connection_limit() -> Result<()> {
    let seed = seed_layout_bytes(32, 1, 64, 64 * 1024, false)?;
    for fast in [false, true] {
        let (_dir, repo, backend) = trial(&seed)?;
        let m = backend.metrics;
        // Two packs need headroom for their final blobs and headers.
        let opts = opts(Some(2), fast).repack_upload_buffer(ByteSize::mib(3));
        let plan = repo.prune_plan(&opts)?;
        m.stall_only_repack_indexes.store(true, SeqCst);
        *m.index_stall.lock().unwrap() = true;
        m.enabled.store(true, SeqCst);
        let worker = thread::spawn(move || {
            let result = repo.prune(&opts, plan);
            (repo, result)
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        while m.repack_index_puts.load(SeqCst) < 2 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        let overlapping = m.repack_index_puts.load(SeqCst);
        thread::sleep(Duration::from_millis(100));
        let gets = m.gets.load(SeqCst);
        thread::sleep(Duration::from_millis(100));
        let stable = m.gets.load(SeqCst) == gets;
        *m.index_stall.lock().unwrap() = false;
        m.index_wake.notify_all();
        let (repo, result) = worker.join().unwrap();
        result?;
        assert_eq!(
            overlapping, 2,
            "stalled index upload blocked other pack workers"
        );
        assert!(
            stable,
            "downloads did not backpressure on stalled index uploads"
        );
        assert!(m.peak.load(SeqCst) <= 2);
        assert_eq!(m.active.load(SeqCst), 0);
        assert!(m.deletes.load(SeqCst) > 0);
        published_after_upload(&repo, &m)?;
        m.enabled.store(false, SeqCst);
        repo.check(CheckOptions::default().read_data(true))?
            .is_ok()?;
    }
    Ok(())
}

#[test]
fn repack_index_failure_preserves_old_data_and_aggregates_worker_errors() -> Result<()> {
    let seed = seed_layout_bytes(32, 1, 64, 64 * 1024, false)?;
    for fast in [false, true] {
        let (_dir, repo, backend) = trial(&seed)?;
        let m = &backend.metrics;
        let old_packs: BTreeSet<_> = backend.list(FileType::Pack)?.into_iter().collect();
        let old_indexes: BTreeSet<_> = backend.list(FileType::Index)?.into_iter().collect();
        let opts = opts(Some(5), fast);
        let plan = repo.prune_plan(&opts)?;
        m.repack_index_failures.store(2, SeqCst);
        m.repack_index_delay_ms.store(300, SeqCst);
        m.enabled.store(true, SeqCst);
        let error = repo.prune(&opts, plan).unwrap_err();
        assert!(
            error.to_string().contains("injected index upload failure"),
            "{error}"
        );
        let failures = 2 - m.repack_index_failures.load(SeqCst);
        assert!(failures > 0);
        assert_eq!(
            error.context_value("upload_failures"),
            Some(failures.to_string().as_str())
        );
        assert!(error.context_value("failed_index").is_some());
        assert!(
            error
                .context_value("upload_errors")
                .unwrap()
                .contains("injected index upload failure")
        );
        assert_eq!(m.deletes.load(SeqCst), 0);
        assert_eq!(m.active.load(SeqCst), 0);
        assert!(old_packs.is_subset(&backend.list(FileType::Pack)?.into_iter().collect()));
        assert!(old_indexes.is_subset(&backend.list(FileType::Index)?.into_iter().collect()));
        published_after_upload(&repo, m)?;
        m.enabled.store(false, SeqCst);
        repo.check(CheckOptions::default().read_data(true))?
            .is_ok()?;
    }
    Ok(())
}

#[test]
fn read_failure_joins_uploads_without_deleting_old_data() -> Result<()> {
    let seed = seed(32)?;
    for fast in [false, true] {
        let (_dir, repo, backend) = trial(&seed)?;
        let m = &backend.metrics;
        let opts = opts(Some(5), fast);
        let plan = repo.prune_plan(&opts)?;
        m.fail_read_at.store(15, SeqCst);
        m.write_ms.store(30, SeqCst);
        m.enabled.store(true, SeqCst);
        let error = repo.prune(&opts, plan).unwrap_err();
        assert!(
            error.to_string().contains("injected pack read failure"),
            "{error}"
        );
        assert!(m.puts.load(SeqCst) > 0);
        assert_eq!(m.deletes.load(SeqCst), 0);
        assert_eq!(m.active.load(SeqCst), 0);
        m.enabled.store(false, SeqCst);
        published_after_upload(&repo, m)?;
        repo.check(CheckOptions::default().read_data(true))?
            .is_ok()?;
    }
    Ok(())
}

#[test]
fn invalid_budget_fails_before_mutating_repository() -> Result<()> {
    let seed = seed(4)?;
    for n in [0, 1] {
        let (_dir, repo, backend) = trial(&seed)?;
        let opts = opts(Some(n), true);
        let plan = repo.prune_plan(&opts)?;
        backend.metrics.enabled.store(true, SeqCst);
        assert!(repo.prune(&opts, plan).is_err());
        assert_eq!(backend.metrics.puts.load(SeqCst), 0);
        assert_eq!(backend.metrics.deletes.load(SeqCst), 0);
        assert!(backend.metrics.published.lock().unwrap().is_empty());
    }
    Ok(())
}

#[test]
fn stalled_uploads_backpressure_downloads_and_resume() -> Result<()> {
    let seed = seed(64)?;
    let (_dir, repo, backend) = trial(&seed)?;
    let m = backend.metrics;
    let opts = opts(Some(5), true);
    let plan = repo.prune_plan(&opts)?;
    *m.stall.lock().unwrap() = true;
    m.enabled.store(true, SeqCst);
    let worker = thread::spawn(move || {
        let result = repo.prune(&opts, plan);
        (repo, result)
    });
    let deadline = Instant::now() + Duration::from_secs(3);
    while m.puts.load(SeqCst) < 5 && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    thread::sleep(Duration::from_millis(200));
    let gets = m.gets.load(SeqCst);
    thread::sleep(Duration::from_millis(200));
    let stable = m.gets.load(SeqCst) == gets;
    *m.stall.lock().unwrap() = false;
    m.wake.notify_all();
    let (repo, result) = worker.join().unwrap();
    result?;
    assert!(stable, "downloads continued while uploads were stalled");
    assert!(
        gets < 64,
        "backpressure arrived only after all source packs were read"
    );
    assert!(m.peak.load(SeqCst) <= 5);
    assert_eq!(m.active.load(SeqCst), 0);
    published_after_upload(&repo, &m)?;
    m.enabled.store(false, SeqCst);
    repo.check(CheckOptions::default().read_data(true))?
        .is_ok()?;
    Ok(())
}

fn benchmark_value(name: &str, default: u64) -> u64 {
    std::env::var(name).map_or(default, |value| {
        value.parse().expect("numeric benchmark parameter")
    })
}

fn benchmark_seed() -> Result<Seed> {
    if std::env::var_os("PRUNE_LAB_REPACK_INDEX_HEAVY").is_some() {
        return seed_layout_bytes(
            usize::try_from(benchmark_value("PRUNE_LAB_MIB", 64))?,
            1,
            64,
            64 * 1024,
            false,
        );
    }
    if std::env::var_os("PRUNE_LAB_INDEX_HEAVY").is_some() {
        return seed_layout_bytes(
            usize::try_from(benchmark_value("PRUNE_LAB_MIB", 64))?,
            1,
            64,
            1024 * 1024,
            true,
        );
    }
    seed_layout(
        usize::try_from(benchmark_value("PRUNE_LAB_MIB", 128))?,
        benchmark_value("PRUNE_LAB_PACK_MIB", 1),
        benchmark_value("PRUNE_LAB_CHUNK_KIB", 64),
    )
}

fn benchmark_fixture() -> Result<Seed> {
    let seed = if let Ok(path) = std::env::var("PRUNE_LAB_SEED") {
        let path = Path::new(&path);
        if path.join("key.json").exists() {
            let dir = tempdir()?;
            copy_dir(&path.join("repo"), &dir.path().join("repo"))?;
            Seed {
                dir,
                key: serde_json::from_slice(&fs::read(path.join("key.json"))?)?,
            }
        } else {
            let seed = benchmark_seed()?;
            copy_dir(&seed.dir.path().join("repo"), &path.join("repo"))?;
            fs::write(path.join("key.json"), serde_json::to_vec(&seed.key)?)?;
            seed
        }
    } else {
        benchmark_seed()?
    };
    Ok(seed)
}

#[test]
#[ignore = "local latency benchmark; run explicitly with --ignored --nocapture"]
#[allow(clippy::cast_precision_loss)]
fn benchmark_prune_pipeline() -> Result<()> {
    let seed = benchmark_fixture()?;
    println!("case,repeat,seconds,read_mib,write_mib,peak_get,peak_put,peak_io,overlap");
    for (name, read_ms, write_ms) in [("local", 0, 0), ("latency", 20, 60)] {
        for repeat in 0..3 {
            let order = if repeat % 2 == 0 {
                [None, Some(5), Some(10)]
            } else {
                [Some(10), Some(5), None]
            };
            for n in order {
                let case = format!(
                    "{name}-{}",
                    n.map_or_else(|| "baseline".into(), |n| n.to_string())
                );
                if std::env::var("PRUNE_LAB_CASE").is_ok_and(|filter| filter != case || repeat > 0)
                {
                    continue;
                }
                let (_dir, repo, backend) = trial(&seed)?;
                let m = &backend.metrics;
                let mut opts = opts(n, std::env::var_os("PRUNE_LAB_FAST_REPACK").is_some())
                    .repack_read_buffer(ByteSize::mib(benchmark_value(
                        "PRUNE_LAB_READ_BUFFER_MIB",
                        128,
                    )))
                    .repack_upload_buffer(ByteSize::mib(benchmark_value(
                        "PRUNE_LAB_UPLOAD_BUFFER_MIB",
                        256,
                    )));
                if std::env::var_os("PRUNE_LAB_INDEX_HEAVY").is_some() {
                    opts.max_repack = "0".parse()?;
                    opts.instant_delete = false;
                    if name == "latency" {
                        m.index_delay_ms
                            .store(benchmark_value("PRUNE_LAB_INDEX_DELAY_MS", 500), SeqCst);
                    }
                }
                if name == "latency" {
                    m.repack_index_delay_ms
                        .store(benchmark_value("PRUNE_LAB_INDEX_DELAY_MS", 0), SeqCst);
                }
                let plan = repo.prune_plan(&opts)?;
                m.read_ms.store(read_ms, SeqCst);
                m.write_ms.store(write_ms, SeqCst);
                if name == "latency" {
                    m.read_mib_per_second
                        .store(benchmark_value("PRUNE_LAB_READ_MIBPS", 0), SeqCst);
                    m.write_mib_per_second
                        .store(benchmark_value("PRUNE_LAB_WRITE_MIBPS", 0), SeqCst);
                }
                m.enabled.store(true, SeqCst);
                println!("BEGIN {case}");
                let start = Instant::now();
                repo.prune(&opts, plan)?;
                let elapsed = start.elapsed().as_secs_f64();
                println!(
                    "{name}-{}, {repeat}, {elapsed:.3}, {:.2}, {:.2}, {}, {}, {}, {}, {}, {}, {:.2}, {}, {}",
                    n.map_or_else(|| "baseline".into(), |n| n.to_string()),
                    m.read_bytes.load(SeqCst) as f64 / 1_048_576.,
                    m.write_bytes.load(SeqCst) as f64 / 1_048_576.,
                    m.peak_reads.load(SeqCst),
                    m.peak_writes.load(SeqCst),
                    m.peak.load(SeqCst),
                    m.overlap.load(SeqCst),
                    m.index_puts.load(SeqCst),
                    m.index_peak.load(SeqCst),
                    m.index_bytes.load(SeqCst) as f64 / 1_048_576.,
                    m.repack_index_puts.load(SeqCst),
                    m.repack_index_peak.load(SeqCst),
                );
                if std::env::var_os("PRUNE_LAB_INDEX_HEAVY").is_some() {
                    assert!(
                        m.index_puts.load(SeqCst) >= 5,
                        "fixture must rebuild multiple output indexes"
                    );
                }
                if std::env::var_os("PRUNE_LAB_REPACK_INDEX_HEAVY").is_some() {
                    assert!(
                        m.repack_index_puts.load(SeqCst) >= 5,
                        "fixture must save multiple indexes during repack"
                    );
                    if let Some(n) = n {
                        assert!(m.peak.load(SeqCst) <= n);
                    }
                }
                published_after_upload(&repo, m)?;
                m.enabled.store(false, SeqCst);
                repo.check(CheckOptions::default().read_data(true))?
                    .is_ok()?;
            }
        }
    }
    Ok(())
}
