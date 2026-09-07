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
    failures: AtomicUsize,
    fail_read_at: AtomicUsize,
    stall: Mutex<bool>,
    wake: Condvar,
    completed: Mutex<BTreeSet<Id>>,
    published: Mutex<Vec<(Id, BTreeSet<Id>)>>,
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
        if self.metrics.fail_read_at.load(SeqCst) == request {
            return Err(RusticError::new(
                ErrorKind::Backend,
                "injected pack read failure",
            ));
        }
        self.metrics.read_bytes.fetch_add(u64::from(l), SeqCst);
        thread::sleep(Duration::from_millis(self.metrics.read_ms.load(SeqCst)));
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
            thread::sleep(Duration::from_millis(self.metrics.write_ms.load(SeqCst)));
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
        } else {
            if t == FileType::Index {
                self.metrics
                    .published
                    .lock()
                    .unwrap()
                    .push((*id, self.metrics.completed.lock().unwrap().clone()));
            }
            self.inner.write_bytes(t, id, c, data)
        }
    }
}

struct Seed {
    dir: TempDir,
    key: MasterKey,
}
fn seed(mebibytes: usize) -> Result<Seed> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    fs::create_dir(&source)?;
    let key = MasterKey::new();
    let backend = LocalBackend::new(dir.path().join("repo").to_str().unwrap(), None)?;
    let backends = RepositoryBackends::new(Arc::new(backend), None);
    let config = ConfigOptions::default()
        .set_chunker(Chunker::FixedSize)
        .set_chunk_size(ByteSize::kib(64))
        .set_datapack_size(ByteSize::mib(1))
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
    for file in 0..mebibytes * 16 {
        let mut data = vec![0u8; 65_536];
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
    for file in (0..mebibytes * 16).step_by(2) {
        fs::remove_file(source.join(format!("{file:06}")))?;
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

#[test]
#[ignore = "local latency benchmark; run explicitly with --ignored --nocapture"]
#[allow(clippy::cast_precision_loss)]
fn benchmark_prune_pipeline() -> Result<()> {
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
            let seed = seed(128)?;
            copy_dir(&seed.dir.path().join("repo"), &path.join("repo"))?;
            fs::write(path.join("key.json"), serde_json::to_vec(&seed.key)?)?;
            seed
        }
    } else {
        seed(128)?
    };
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
                let opts = opts(n, false);
                let plan = repo.prune_plan(&opts)?;
                m.read_ms.store(read_ms, SeqCst);
                m.write_ms.store(write_ms, SeqCst);
                m.enabled.store(true, SeqCst);
                println!("BEGIN {case}");
                let start = Instant::now();
                repo.prune(&opts, plan)?;
                let elapsed = start.elapsed().as_secs_f64();
                println!(
                    "{name}-{}, {repeat}, {elapsed:.3}, {:.2}, {:.2}, {}, {}, {}, {}",
                    n.map_or_else(|| "baseline".into(), |n| n.to_string()),
                    m.read_bytes.load(SeqCst) as f64 / 1_048_576.,
                    m.write_bytes.load(SeqCst) as f64 / 1_048_576.,
                    m.peak_reads.load(SeqCst),
                    m.peak_writes.load(SeqCst),
                    m.peak.load(SeqCst),
                    m.overlap.load(SeqCst)
                );
                published_after_upload(&repo, m)?;
                m.enabled.store(false, SeqCst);
                repo.check(CheckOptions::default().read_data(true))?
                    .is_ok()?;
            }
        }
    }
    Ok(())
}
