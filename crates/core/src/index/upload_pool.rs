//! Bounded index serialization handoff for the prune rebuild phase.
use crate::{
    backend::{FileType, decrypt::DecryptWriteBackend},
    blob::{
        byte_budget::{ByteBudget, BytePermit},
        upload_pool::StopSignal,
    },
    error::{ErrorKind, RusticError, RusticResult},
    repofile::indexfile::IndexFile,
};
use crossbeam_channel::{Sender, bounded, select_biased};
use std::{
    fmt,
    sync::{Arc, Mutex},
    thread::JoinHandle,
};

type Job = (Box<[u8]>, BytePermit);

pub(super) struct IndexUploadPool {
    sender: Option<Sender<Job>>,
    workers: Vec<JoinHandle<()>>,
    stopped: StopSignal,
    budget: ByteBudget,
    errors: Arc<Mutex<Vec<RusticError>>>,
}
impl fmt::Debug for IndexUploadPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IndexUploadPool")
            .field("workers", &self.workers.len())
            .finish_non_exhaustive()
    }
}
impl IndexUploadPool {
    pub(super) fn new<BE: DecryptWriteBackend>(be: BE, n: usize, bytes: u64) -> RusticResult<Self> {
        Self::with_writer(n, bytes, move |data| {
            be.hash_write_full(FileType::Index, data).map(|_| ())
        })
    }
    fn with_writer<W>(n: usize, bytes: u64, write: W) -> RusticResult<Self>
    where
        W: Fn(&[u8]) -> RusticResult<()> + Clone + Send + 'static,
    {
        let (sender, receiver) = bounded::<Job>(0);
        let mut pool = Self {
            sender: Some(sender),
            workers: Vec::new(),
            stopped: StopSignal::new(),
            budget: ByteBudget::new(bytes, "--repack-upload-buffer"),
            errors: Arc::new(Mutex::new(Vec::new())),
        };
        for _ in 0..n {
            let (receiver, stopped, budget, errors, write) = (
                receiver.clone(),
                pool.stopped.clone(),
                pool.budget.clone(),
                pool.errors.clone(),
                write.clone(),
            );
            let worker = std::thread::Builder::new()
                .name("prune-index".into())
                .spawn(move || {
                    loop {
                        let (data, _permit) = select_biased! {
                            recv(stopped.receiver()) -> _ => break,
                            recv(receiver) -> job => match job { Ok(job) => job, Err(_) => break },
                        };
                        if stopped.check().is_err() {
                            break;
                        }
                        if let Err(error) = write(&data) {
                            errors.lock().unwrap().push(*error);
                            stopped.cancel();
                            budget.cancel();
                        }
                    }
                })
                .map_err(|error| {
                    RusticError::with_source(
                        ErrorKind::Internal,
                        "Cannot start prune index upload worker.",
                        error,
                    )
                })?;
            pool.workers.push(worker);
        }
        Ok(pool)
    }
    pub(super) fn save(&self, file: &IndexFile) -> RusticResult<()> {
        self.stopped.check()?;
        let data = serde_json::to_vec(file)
            .map_err(|error| {
                RusticError::with_source(
                    ErrorKind::Internal,
                    "Failed to serialize index to JSON.",
                    error,
                )
            })?
            .into_boxed_slice();
        let permit = self.budget.acquire(data.len() as u64)?;
        let sender = self
            .sender
            .as_ref()
            .expect("index submission precedes finalization");
        select_biased! {
            recv(self.stopped.receiver()) -> _ => return Err(StopSignal::error()),
            send(sender, (data, permit)) -> result => result.map_err(|_| RusticError::new(ErrorKind::Backend, "Index upload pool disconnected."))?,
        }
        self.stopped.check()
    }
    fn join(&mut self) {
        drop(self.sender.take());
        for worker in self.workers.drain(..) {
            if worker.join().is_err() {
                self.errors.lock().unwrap().push(*RusticError::new(
                    ErrorKind::Internal,
                    "Index upload worker panicked.",
                ));
            }
        }
    }
    pub(super) fn finalize(mut self) -> RusticResult<()> {
        self.join();
        log::debug!("index upload peak: {} serialized bytes", self.budget.peak());
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
            .attach_context("index_upload_failures", count.to_string())
            .attach_context("index_upload_errors", details))
    }
}
impl Drop for IndexUploadPool {
    fn drop(&mut self) {
        self.stopped.cancel();
        self.budget.cancel();
        self.join();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repofile::indexfile::IndexPack;
    use std::{
        sync::{
            Condvar,
            atomic::{AtomicUsize, Ordering::SeqCst},
        },
        time::{Duration, Instant},
    };

    #[test]
    fn stalled_uploads_bound_admitted_bytes_and_resume() {
        let file = IndexFile {
            packs: vec![IndexPack::default()],
            ..Default::default()
        };
        let size = serde_json::to_vec(&file).unwrap().len() as u64;
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let started = Arc::new(AtomicUsize::new(0));
        let pool = IndexUploadPool::with_writer(5, 2 * size, {
            let gate = gate.clone();
            let started = started.clone();
            move |data| {
                assert_eq!(data.len() as u64, size);
                let _: IndexFile = serde_json::from_slice(data).unwrap();
                _ = started.fetch_add(1, SeqCst);
                let (open, _) = gate
                    .1
                    .wait_timeout_while(gate.0.lock().unwrap(), Duration::from_secs(5), |open| {
                        !*open
                    })
                    .unwrap();
                assert!(*open, "test index upload timed out");
                Ok(())
            }
        })
        .unwrap();
        let budget = pool.budget.clone();
        let worker = std::thread::spawn(move || {
            for _ in 0..8 {
                pool.save(&file).unwrap();
            }
            pool.finalize().unwrap();
        });
        let deadline = Instant::now() + Duration::from_secs(2);
        while started.load(SeqCst) < 2 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        std::thread::sleep(Duration::from_millis(30));
        let blocked_count = started.load(SeqCst);
        *gate.0.lock().unwrap() = true;
        gate.1.notify_all();
        worker.join().unwrap();
        assert_eq!(
            blocked_count, 2,
            "producer exceeded the byte budget while uploads were stalled"
        );
        assert_eq!(started.load(SeqCst), 8);
        assert_eq!(budget.peak(), 2 * size);
    }

    #[test]
    fn oversized_index_is_rejected_before_upload() {
        let pool =
            IndexUploadPool::with_writer(2, 1, |_| panic!("oversized index was uploaded")).unwrap();
        let err = pool.save(&IndexFile::default()).unwrap_err();
        assert_eq!(err.context_value("option"), Some("--repack-upload-buffer"));
        assert!(err.context_value("required_bytes").is_some());
        assert_eq!(pool.budget.peak(), 0);
        pool.finalize().unwrap();
    }
}
