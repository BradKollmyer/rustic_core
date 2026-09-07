//! Opt-in prune upload pool. Pack bodies share one rendezvous queue and I/O budget.

use super::{
    byte_budget::{BytePermit, RepackBuffers},
    packer::FileWriterHandle,
};
use crate::{
    backend::{BytesList, decrypt::DecryptWriteBackend},
    crypto::hasher::hash_reader,
    error::{ErrorKind, RusticError, RusticResult},
    index::indexer::SharedIndexer,
    repofile::{indexfile::IndexPack, packfile::PackId},
};
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, bounded};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};

#[derive(Clone)]
pub(crate) struct IoBudget {
    tx: Sender<()>,
    rx: Receiver<()>,
}
impl IoBudget {
    pub(crate) fn new(n: usize) -> Self {
        let (tx, rx) = bounded(n);
        for _ in 0..n {
            tx.send(()).unwrap();
        }
        Self { tx, rx }
    }
    pub(crate) fn acquire(&self) -> IoPermit<'_> {
        self.rx.recv().unwrap();
        IoPermit(self)
    }
}
pub(crate) struct IoPermit<'a>(&'a IoBudget);
impl Drop for IoPermit<'_> {
    fn drop(&mut self) {
        self.0.tx.send(()).unwrap();
    }
}

type Job = (BytesList, IndexPack, bool, BytePermit);
#[derive(Clone)]
pub(crate) struct UploadSender {
    tx: Sender<Job>,
    stopped: Arc<AtomicBool>,
    buffers: RepackBuffers,
}
impl UploadSender {
    pub(crate) fn reserve(&self, bytes: u64) -> RusticResult<BytePermit> {
        if self.stopped.load(Ordering::Acquire) {
            return Err(RusticError::new(
                ErrorKind::Backend,
                "Pack upload pool stopped after a failure.",
            ));
        }
        self.buffers.uploads.acquire(bytes)
    }

    pub(crate) fn send(
        &self,
        file: BytesList,
        index: IndexPack,
        cacheable: bool,
        bytes: BytePermit,
    ) -> RusticResult<()> {
        self.tx
            .send((file, index, cacheable, bytes))
            .map_err(|_| RusticError::new(ErrorKind::Backend, "Pack upload pool disconnected."))
    }
}

pub(crate) struct UploadPool {
    sender: Option<UploadSender>,
    workers: Vec<JoinHandle<()>>,
    stopped: Arc<AtomicBool>,
    errors: Arc<Mutex<Vec<RusticError>>>,
    buffers: RepackBuffers,
}
impl UploadPool {
    pub(crate) fn new<BE: DecryptWriteBackend>(
        be: &BE,
        indexer: &SharedIndexer<BE>,
        n: usize,
        budget: &IoBudget,
        buffers: &RepackBuffers,
    ) -> Self {
        let (tx, rx) = bounded::<Job>(0);
        let stopped = Arc::new(AtomicBool::new(false));
        let errors = Arc::new(Mutex::new(Vec::new()));
        let mut workers = Vec::new();
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
            workers.push(std::thread::spawn(move || {
                while !stopped.load(Ordering::Acquire) {
                    let (file, index, cacheable, _bytes) =
                        match rx.recv_timeout(Duration::from_millis(50)) {
                            Ok(job) => job,
                            Err(RecvTimeoutError::Timeout) => continue,
                            Err(RecvTimeoutError::Disconnected) => break,
                        };
                    if stopped.load(Ordering::Acquire) {
                        break;
                    }
                    let id = PackId::from(
                        hash_reader(file.clone().reader()).expect("reading memory cannot fail"),
                    );
                    let _permit = budget.acquire();
                    if stopped.load(Ordering::Acquire) {
                        break;
                    }
                    let writer = FileWriterHandle {
                        be: be.clone(),
                        indexer: indexer.clone(),
                        cacheable,
                    };
                    let result = writer
                        .process((file, id, index))
                        .and_then(|index| writer.index(index));
                    if let Err(error) = result {
                        errors
                            .lock()
                            .unwrap()
                            .push(*error.attach_context("pack_id", id.to_string()));
                        stopped.store(true, Ordering::Release);
                        buffers.cancel();
                    }
                }
            }));
        }
        Self {
            sender: Some(UploadSender {
                tx,
                stopped: stopped.clone(),
                buffers: buffers.clone(),
            }),
            workers,
            stopped,
            errors,
            buffers: buffers.clone(),
        }
    }
    pub(crate) fn sender(&self) -> UploadSender {
        self.sender.as_ref().unwrap().clone()
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
        self.stopped.store(true, Ordering::Release);
        self.buffers.cancel();
        self.join();
    }
}
