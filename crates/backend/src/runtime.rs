//! Shared Tokio runtime for backends that call async APIs from sync traits.
use std::sync::OnceLock;

use tokio::runtime::Runtime;

/// Process-wide multi-thread Tokio runtime.
///
/// REST, `OpenDAL`, and Storj all need a runtime to `block_on` async work from
/// the sync `ReadBackend` / `WriteBackend` traits. Sharing one avoids nested
/// runtimes when more than one of those backends is used in the same process.
pub(crate) fn runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to create Tokio runtime for rustic backends")
    })
}
