//! Weighted permits for the two independent repack buffer stages.
use crate::error::{ErrorKind, RusticError, RusticResult};
use std::sync::{Arc, Condvar, Mutex};

#[derive(Default)]
struct State {
    used: u64,
    peak: u64,
    cancelled: bool,
}
struct Inner {
    limit: u64,
    option: &'static str,
    state: Mutex<State>,
    wake: Condvar,
}
#[derive(Clone)]
pub(crate) struct ByteBudget(Arc<Inner>);
pub(crate) struct BytePermit {
    budget: ByteBudget,
    bytes: u64,
}
impl ByteBudget {
    pub(crate) fn new(limit: u64, option: &'static str) -> Self {
        Self(Arc::new(Inner {
            limit,
            option,
            state: Mutex::new(State::default()),
            wake: Condvar::new(),
        }))
    }
    pub(crate) fn limit(&self) -> u64 {
        self.0.limit
    }
    pub(crate) fn acquire(&self, bytes: u64) -> RusticResult<BytePermit> {
        if bytes > self.0.limit {
            return Err(RusticError::new(
                ErrorKind::InvalidInput,
                "A repack buffer exceeds its byte budget; increase the indicated option.",
            )
            .attach_context("option", self.0.option)
            .attach_context("required_bytes", bytes.to_string())
            .attach_context("budget_bytes", self.0.limit.to_string()));
        }
        let mut state = self.0.state.lock().unwrap();
        while !state.cancelled && bytes > self.0.limit - state.used {
            state = self.0.wake.wait(state).unwrap();
        }
        if state.cancelled {
            return Err(RusticError::new(
                ErrorKind::Backend,
                "Repack buffer wait cancelled after a failure.",
            ));
        }
        state.used += bytes;
        state.peak = state.peak.max(state.used);
        drop(state);
        Ok(BytePermit {
            budget: self.clone(),
            bytes,
        })
    }
    pub(crate) fn cancel(&self) {
        self.0.state.lock().unwrap().cancelled = true;
        self.0.wake.notify_all();
    }
    pub(crate) fn peak(&self) -> u64 {
        self.0.state.lock().unwrap().peak
    }
}
impl Drop for BytePermit {
    fn drop(&mut self) {
        self.budget.0.state.lock().unwrap().used -= self.bytes;
        self.budget.0.wake.notify_all();
    }
}

#[derive(Clone)]
pub(crate) struct RepackBuffers {
    pub(crate) reads: ByteBudget,
    pub(crate) uploads: ByteBudget,
}
impl RepackBuffers {
    pub(crate) fn new(reads: u64, uploads: u64) -> Self {
        Self {
            reads: ByteBudget::new(reads, "--repack-read-buffer"),
            uploads: ByteBudget::new(uploads, "--repack-upload-buffer"),
        }
    }
    pub(crate) fn cancel(&self) {
        self.reads.cancel();
        self.uploads.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::mpsc, thread, time::Duration};

    #[test]
    fn byte_wait_resumes_on_release_and_never_exceeds_limit() {
        let budget = ByteBudget::new(10, "test");
        let first = budget.acquire(7).unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let other = budget.clone();
        let worker = thread::spawn(move || {
            started_tx.send(()).unwrap();
            let permit = other.acquire(4).unwrap();
            done_tx.send(()).unwrap();
            drop(permit);
        });
        started_rx.recv().unwrap();
        assert!(done_rx.recv_timeout(Duration::from_millis(30)).is_err());
        drop(first);
        done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        worker.join().unwrap();
        assert_eq!(budget.peak(), 7);
        assert_eq!(budget.0.state.lock().unwrap().used, 0);
        assert!(budget.acquire(11).is_err());
    }

    #[test]
    fn cancellation_releases_waiters_and_budgets_do_not_depend_on_each_other() {
        let buffers = RepackBuffers::new(10, 20);
        let read = buffers.reads.acquire(10).unwrap();
        let upload = buffers.uploads.acquire(20).unwrap();
        let other = buffers.clone();
        let worker = thread::spawn(move || other.reads.acquire(1).is_err());
        buffers.cancel();
        assert!(worker.join().unwrap());
        assert!(buffers.uploads.acquire(0).is_err());
        drop((read, upload));
        assert_eq!(buffers.reads.0.state.lock().unwrap().used, 0);
        assert_eq!(buffers.uploads.0.state.lock().unwrap().used, 0);
    }
}
