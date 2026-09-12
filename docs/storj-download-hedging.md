# Native Storj restore: bounded speculative piece downloads

The native SDK previously started `k+1` piece downloads and only tried another
node after a transfer failed. Two silent nodes could hold a pack until the
20-second per-message timeout; a slow node making progress could delay it longer.

The scheduler now tries one additional node after one second without a completed
piece. Existing transfers keep running. Each completion resets the delay, and
ready completions take priority over starting another transfer. As soon as `k`
pieces succeed, surplus tasks are cancelled and drained using the existing
connection-pool safeguards. Cancelled connections are not reused mid-RPC.

Speculative spares, including the initial margin, are capped at
`min(k, max(2, ceil(k/5)))`. For production `k=29`, this means at most 35 piece
attempts without failures, versus 30 before. Failure replacements can add further
attempts, as before. This trades some additional traffic for shorter waits on
slow nodes; it does not impose a deadline on the whole piece or remove the
existing object concurrency limit. Corruption recovery still fetches additional
shares through its existing sequential fallback.

## Configuration

```toml
[repository.options]
connections = "5"
message-timeout = "20"
download-hedge-delay = "1" # seconds; default 1, 0 disables speculation
```

SDK callers can set `Config.download_hedge_delay` with subsecond precision.
`None` uses one second; `Some(Duration::ZERO)` disables speculation.

## Dependency and build

The SDK changes are committed as
[`e635fba`](https://github.com/BradKollmyer/storj-uplink/commit/e635fba246c15e6bee7fe9e1a4c31207a6e5915b),
on branch `fix/storj-download-hedging`, based on `8d9eaa7`. Both the rustic CLI
and rustic_core pin that Git revision. The CLI also pins the corresponding
rustic_core implementation so builds do not depend on local checkout paths.
The prior concurrency and timeout fixes are included in rustic_core.

The built CLI is `../rustic/target-darwin-dbg/release/rustic`, stamped
`v0.11.4-15-g9993998+storj-8d9eaa7-hedge+core-14f2aad-dirty`.

```sh
cd ../rustic
CARGO_PROFILE_RELEASE_LTO=false \
CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 \
CARGO_TARGET_DIR=target-darwin-dbg \
PROJECT_VERSION=v0.11.4-15-g9993998+storj-8d9eaa7-hedge+core-14f2aad-dirty \
cargo build --locked --release --features release --bin rustic
```

## Validation

The download scheduler tests cover two slow pieces without failures, healthy
downloads that need no speculation, the 35-attempt production cap, disabling
speculation, failure replacements after the speculative budget is spent, and
cancellation of surplus tasks. Existing range, reconstruction, corrupt-share,
connection-pool, API-contract and mock-fault tests also pass: 156 tests passed,
with two pre-existing ignored tests (an unfinished commit-timeout test and an
opt-in live upload test). SDK Clippy passes with warnings denied.

```sh
# SDK checkout
cargo test --offline -p storj-uplink --lib
cargo test --offline -p storj --lib --test api_contract --test mock_faults
# rustic_core
cargo test --offline -p rustic_backend --no-default-features --features storj --lib storj::tests
```

## Live restore measurements, 2026-09-12

The benchmark reads snapshot `59bff332` from `storj:photos`, restoring
`/var/mnt/photos/2026-08-31/7P5A3626.CR3` to a fresh filename on each run. The
file is 16,134,742 bytes; its expected MD5 is
`7780fbb83f027b7cc5a4b90b9b3c9874`.

Both settings use five object connections, a 20-second message timeout, the same
metadata cache, and a per-process open-file limit of 8192. A preliminary run
failed with `Too many open files` under the shell's default limit; it is excluded
from timings. The first successful baseline filled the metadata cache, so compare
file-content times rather than its overall wall time. File-data range reads in
rustic restore bypass the disk cache.

| Run | File contents | Wall | MD5 |
|---|---:|---:|---|
| Original binary | 89.66 s | 186.40 s (cold metadata) | Matches |
| Updated binary, default delay | 24.35 s | 25.57 s | Matches |
| Updated binary, delay 0 (disabled), first repeat | 229.17 s | 230.38 s | Matches |
| Updated binary, default delay, second repeat | 8.76 s | 10.00 s | Matches |
| Updated binary, delay 0 (disabled), second repeat | 224.13 s | 225.35 s | Matches |

With the same updated binary, the two enabled runs took 8.76–24.35 seconds for
file contents, versus 224.13–229.17 seconds with speculation disabled. All five
completed restores match the original hash. These small samples demonstrate a
large reduction in this file's slow-node wait, not a guaranteed speedup for every
file or network condition. Speculative-transfer bytes were not measured.

Raw logs, profiles, restored copies and the original binary
are in `/private/tmp/rustic-storj-perf.S9fUcA/`.

This is a single-file benchmark. A complete 14 GiB restore and deployment to arc
have not been performed with the updated SDK.
