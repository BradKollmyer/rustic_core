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

The benchmark CLI is `../rustic/target-darwin-dbg/release/rustic`, stamped
`v0.11.4-15-g9993998+storj-8d9eaa7-hedge+core-14f2aad-dirty`.

```sh
cd ../rustic
CARGO_PROFILE_RELEASE_LTO=false \
CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 \
CARGO_TARGET_DIR=target-darwin-dbg \
PROJECT_VERSION="$(git describe --tags --always)+storj-e635fba+core-acd990e" \
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

## Full-day restore, 2026-09-12

The complete `2026-08-31` photo directory was restored from snapshot `59bff332`
on the same Mac, with the default one-second hedge delay, five object connections
and a 20-second message timeout. The destination was new; the run did not reuse
existing restored files. It used a warm metadata cache, `--no-ownership`, and
`ulimit -n 8192`.

| | Earlier native run | Native with hedging |
|---|---:|---:|
| Files | 814 | 814 |
| Logical bytes | 15,107,783,814 | 15,107,783,814 |
| Wall time | 4,748 s (79.13 min) | 1,151.26 s (19.19 min) |
| Payload throughput | 3.03 MiB/s | 12.51 MiB/s |
| Exit status | 0 | 0 |

The full restore was **4.12× faster**, saving **59.95 minutes**. File-content
restoration took 1,149.79 seconds; metadata took 178 ms. After completion,
`diff -rq` compared all 814 files against the earlier restored directory and
exited 0 with no differences. Both directories contain exactly 15,107,783,814
bytes (14.07 GiB). This verifies file contents, not ownership metadata.

The baseline is the earlier 07:06:48–08:25:56 UTC run recorded in
`/Users/bradk/photos-restore/perf/restore-wrapper.log`; it is not a simultaneous
control. The updated restore finished at 20:43:59 UTC. Git/Cargo dependency checks
ran during its first few minutes. Network conditions can vary between runs.

```sh
ulimit -n 8192
/usr/bin/time -p ./target-darwin-dbg/release/rustic \
  -P /private/tmp/rustic-storj-perf.S9fUcA/native \
  restore 59bff332:/var/mnt/photos/2026-08-31 \
  /Users/bradk/repos/rustic/restore/photos-hedged.r9h5lJ/2026-08-31 \
  --no-ownership \
  --log-file /Users/bradk/repos/rustic/restore/photos-hedged.r9h5lJ/full-day.log

diff -rq /Users/bradk/photos-restore/2026-08-31 \
  /Users/bradk/repos/rustic/restore/photos-hedged.r9h5lJ/2026-08-31
```

The command above records this run; use a fresh destination for another timing.
The restored files and full log remain under
`/Users/bradk/repos/rustic/restore/photos-hedged.r9h5lJ/`.
The updated SDK has not been deployed to arc.
