//! Opt-in prune upload pool. Pack bodies share one rendezvous queue and I/O budget.

use super::{
    byte_budget::{ByteBudget, BytePermit, RepackBuffers},
    packer::FileWriterHandle,
};
use crate::{
    backend::{BytesList, decrypt::DecryptWriteBackend},
    crypto::hasher::hash_reader,
    error::{ErrorKind, RusticError, RusticResult},
    index::indexer::SharedIndexer,
    repofile::{indexfile::IndexPack, packfile::PackId},
};
use crossbeam_channel::{Receiver, Sender, TryRecvError, bounded, select_biased};
use std::{
    sync::{Arc, Mutex},
    thread::JoinHandle,
};

#[derive(Clone)]
pub(crate) struct IoBudget(ByteBudget);
impl IoBudget {
    pub(crate) fn new(n: usize) -> Self {
        Self(ByteBudget::new(n as u64, "--repack-connections"))
    }
    pub(crate) fn acquire(&self) -> RusticResult<BytePermit> {
        self.0.acquire(1)
    }
    fn cancel(&self) {
        self.0.cancel();
    }
}

#[derive(Clone)]
pub(crate) struct StopSignal {
    sender: Arc<Mutex<Option<Sender<()>>>>,
    receiver: Receiver<()>,
}
impl StopSignal {
    pub(crate) fn new() -> Self {
        let (sender, receiver) = bounded(0);
        Self {
            sender: Arc::new(Mutex::new(Some(sender))),
            receiver,
        }
    }
    pub(crate) fn receiver(&self) -> &Receiver<()> {
        &self.receiver
    }
    pub(crate) fn cancel(&self) {
        // Disconnect broadcasts cancellation to every sender and worker.
        drop(self.sender.lock().unwrap().take());
    }
    pub(crate) fn check(&self) -> RusticResult<()> {
        if matches!(self.receiver.try_recv(), Err(TryRecvError::Empty)) {
            Ok(())
        } else {
            Err(Self::error())
        }
    }
    pub(crate) fn error() -> Box<RusticError> {
        RusticError::new(ErrorKind::Backend, "Upload pool stopped after a failure.")
    }
}

type Job = (BytesList, IndexPack, bool, BytePermit);
#[derive(Clone)]
pub(crate) struct UploadSender {
    tx: Sender<Job>,
    stopped: StopSignal,
    buffers: RepackBuffers,
}
impl UploadSender {
    pub(crate) fn reserve(&self, bytes: u64) -> RusticResult<BytePermit> {
        self.stopped.check()?;
        self.buffers.uploads.acquire(bytes)
    }

    pub(crate) fn send(
        &self,
        file: BytesList,
        index: IndexPack,
        cacheable: bool,
        bytes: BytePermit,
    ) -> RusticResult<()> {
        self.stopped.check()?;
        select_biased! {
            recv(self.stopped.receiver) -> _ => return Err(StopSignal::error()),
            send(self.tx, (file, index, cacheable, bytes)) -> result => {
                result.map_err(|_| RusticError::new(ErrorKind::Backend, "Pack upload pool disconnected."))?;
            }
        }
        self.stopped.check()
    }
}

pub(crate) struct UploadPool {
    sender: Option<UploadSender>,
    workers: Vec<JoinHandle<()>>,
    stopped: StopSignal,
    errors: Arc<Mutex<Vec<RusticError>>>,
    budget: IoBudget,
    buffers: RepackBuffers,
}
impl UploadPool {
    pub(crate) fn new<BE: DecryptWriteBackend>(
        be: &BE,
        indexer: &SharedIndexer<BE>,
        n: usize,
        budget: &IoBudget,
        buffers: &RepackBuffers,
    ) -> RusticResult<Self> {
        Self::with_spawner(be, indexer, n, budget, buffers, |job| {
            std::thread::Builder::new()
                .name("prune-pack".into())
                .spawn(job)
        })
    }

    fn with_spawner<BE: DecryptWriteBackend>(
        be: &BE,
        indexer: &SharedIndexer<BE>,
        n: usize,
        budget: &IoBudget,
        buffers: &RepackBuffers,
        mut spawn: impl FnMut(Box<dyn FnOnce() + Send>) -> std::io::Result<JoinHandle<()>>,
    ) -> RusticResult<Self> {
        let (tx, rx) = bounded::<Job>(0);
        let stopped = StopSignal::new();
        let errors = Arc::new(Mutex::new(Vec::new()));
        let mut pool = Self {
            sender: Some(UploadSender {
                tx,
                stopped: stopped.clone(),
                buffers: buffers.clone(),
            }),
            workers: Vec::new(),
            stopped: stopped.clone(),
            errors: errors.clone(),
            budget: budget.clone(),
            buffers: buffers.clone(),
        };

        for _ in 0..n {
            let (rx, stopped, errors, budget, be, indexer) = (
                rx.clone(),
                stopped.clone(),
                errors.clone(),
                budget.clone(),
                be.clone(),
                indexer.clone(),
            );
            let buffers = buffers.clone();
            let worker = spawn(Box::new(move || {
                loop {
                    let (file, index, cacheable, _bytes) = select_biased! {
                        recv(stopped.receiver) -> _ => break,
                        recv(rx) -> job => match job {
                            Ok(job) => job,
                            Err(_) => break,
                        },
                    };
                    if stopped.check().is_err() {
                        break;
                    }
                    let id = PackId::from(
                        hash_reader(file.clone().reader()).expect("reading memory cannot fail"),
                    );
                    let Ok(_permit) = budget.acquire() else {
                        break;
                    };
                    if stopped.check().is_err() {
                        break;
                    }
                    let writer = FileWriterHandle {
                        be: be.clone(),
                        indexer: indexer.clone(),
                        cacheable,
                    };
                    let result = writer
                        .process((file, id, index))
                        .and_then(|index| writer.index_parallel(index));
                    if let Err(error) = result {
                        errors
                            .lock()
                            .unwrap()
                            .push(*error.attach_context("pack_id", id.to_string()));
                        stopped.cancel();
                        budget.cancel();
                        buffers.cancel();
                    }
                }
            }))
            .map_err(|error| {
                RusticError::with_source(
                    ErrorKind::Internal,
                    "Cannot start prune pack upload worker.",
                    error,
                )
            })?;
            pool.workers.push(worker);
        }
        Ok(pool)
    }
    pub(crate) fn sender(&self) -> UploadSender {
        self.sender
            .as_ref()
            .expect("sender requested before pool finalization")
            .clone()
    }
    fn join(&mut self) {
        drop(self.sender.take());
        for worker in self.workers.drain(..) {
            if worker.join().is_err() {
                self.errors.lock().unwrap().push(*RusticError::new(
                    ErrorKind::Internal,
                    "Pack upload worker panicked.",
                ));
            }
        }
    }
    pub(crate) fn finalize(mut self) -> RusticResult<()> {
        self.join();
        log::debug!(
            "repack buffer peaks: reads {} bytes, uploads {} bytes",
            self.buffers.reads.peak(),
            self.buffers.uploads.peak()
        );
        let mut errors = self.errors.lock().unwrap();
        if errors.is_empty() {
            return Ok(());
        }
        let details = errors
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("; ");
        let count = errors.len();
        Err(errors
            .remove(0)
            .attach_context("upload_failures", count.to_string())
            .attach_context("upload_errors", details))
    }
}
impl Drop for UploadPool {
    fn drop(&mut self) {
        self.stopped.cancel();
        self.budget.cancel();
        self.buffers.cancel();
        self.join();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn spawn_failure_joins_previously_started_workers() {
        use crate::{
            backend::{MockBackend, decrypt::DecryptBackend},
            crypto::aespoly1305::Key,
            index::indexer::Indexer,
        };
        use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
        let be = DecryptBackend::new(Arc::new(MockBackend::new()), Key::new());
        let indexer = Indexer::new_unindexed(be.clone()).into_shared();
        let budget = IoBudget::new(5);
        let buffers = RepackBuffers::new(10, 10);
        let finished = Arc::new(AtomicUsize::new(0));
        let mut calls = 0;
        let error = UploadPool::with_spawner(&be, &indexer, 5, &budget, &buffers, |job| {
            calls += 1;
            if calls == 3 {
                return Err(std::io::Error::other("injected thread creation failure"));
            }
            let finished = finished.clone();
            std::thread::Builder::new().spawn(move || {
                job();
                finished.fetch_add(1, SeqCst);
            })
        })
        .err()
        .expect("third spawn must fail");
        assert!(
            error
                .to_string()
                .contains("Cannot start prune pack upload worker")
        );
        assert_eq!(finished.load(SeqCst), 2);
        assert!(budget.acquire().is_err());
    }

    #[test]
    fn cancellation_rejects_reserved_upload_without_disconnecting_workers() {
        let (tx, rx) = bounded(1);
        let stopped = StopSignal::new();
        let buffers = RepackBuffers::new(1, 1);
        let sender = UploadSender {
            tx,
            stopped: stopped.clone(),
            buffers: buffers.clone(),
        };
        let bytes = sender.reserve(1).unwrap();
        stopped.cancel();
        assert!(
            sender
                .send(vec![0].into(), IndexPack::default(), false, bytes)
                .is_err()
        );
        assert!(
            rx.try_recv().is_err(),
            "cancelled job was handed to a worker"
        );
        assert!(sender.reserve(1).is_err());
        // The rejected job returned its bytes even though the worker is still alive.
        assert!(buffers.uploads.acquire(1).is_ok());
    }

    #[test]
    fn cancellation_wakes_all_blocked_upload_senders() {
        let (tx, rx) = bounded(0);
        let stopped = StopSignal::new();
        let buffers = RepackBuffers::new(1, 3);
        let sender = UploadSender {
            tx,
            stopped: stopped.clone(),
            buffers: buffers.clone(),
        };
        let (done_tx, done_rx) = bounded(3);
        let mut threads = Vec::new();
        for _ in 0..3 {
            let bytes = sender.reserve(1).unwrap();
            let sender = sender.clone();
            let done = done_tx.clone();
            threads.push(std::thread::spawn(move || {
                let result = sender.send(vec![0].into(), IndexPack::default(), false, bytes);
                let _ = done.send(result.is_err());
            }));
        }
        assert!(done_rx.recv_timeout(Duration::from_millis(30)).is_err());
        stopped.cancel();
        let results: Vec<_> = (0..3)
            .map(|_| done_rx.recv_timeout(Duration::from_secs(1)))
            .collect();
        // Keep the work receiver connected until every sender has been woken by cancellation.
        drop(rx);
        for thread in threads {
            thread.join().unwrap();
        }
        for result in results {
            assert!(result.unwrap());
        }
        assert!(buffers.uploads.acquire(3).is_ok());
    }

    #[test]
    fn cancellation_wakes_io_waiter_without_releasing_active_permit() {
        let budget = IoBudget::new(1);
        let active = budget.acquire().unwrap();
        let other = budget.clone();
        let (done_tx, done_rx) = bounded(0);
        let waiter = std::thread::spawn(move || {
            let _ = done_tx.send(other.acquire().is_err());
        });
        assert!(done_rx.recv_timeout(Duration::from_millis(30)).is_err());
        budget.cancel();
        let cancelled = done_rx.recv_timeout(Duration::from_secs(1));
        drop(active);
        assert!(cancelled.unwrap());
        waiter.join().unwrap();
        assert!(budget.acquire().is_err());
    }
}
