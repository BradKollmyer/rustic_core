# External review follow-ups, September 8

This addresses the findings pasted from the two external reviews of `dev`.
The full temporary review file was no longer present on this host, so findings
not included in the pasted text were not assessed.

| Finding | Resolution | Commit |
|---|---|---|
| Storj typed retry API depends on an uncommitted local path | Commit an exact, remotely available Git revision and its lockfile; retain typed retry classification | `351ada6` |
| Strict Clippy fails on descriptor-cache bounds | Use the equivalent `clamp` expression | `38b1534` |
| Reentrant decompression callback panics on the TLS borrow | Nested calls use temporary decompressor/buffer state; ordinary calls keep TLS reuse | `9809e3c` |
| Poisoned budget mutex can panic during permit cleanup | Recover state, cancel the budget, wake waiters, and allow permit cleanup | `57803cd` |
| Pack worker creation can panic and abandon partial startup | Use fallible named spawning and RAII cleanup; test failure on the third spawn | `485b249`, test lint correction `c64b3ca` |
| Tree loaders ignore advertised connection limits | Cap default and cached loader counts, including warm-cache misses | `6366087` |
| Clearly undersized upload budgets fail after substantial work | Check planned pack targets before warmup or prune mutation | `b325661` |
| Stale callback comment and undocumented chunk-lookup cost | Describe per-loader callback state and the O(K log C) lookup tradeoff | This documentation change |

## Boundaries

The Storj root-workspace patch points to
`https://github.com/BradKollmyer/storj-uplink.git` at
`f4e5374e524da86dcd4cde38c302813df173dba9`. A default backend build was verified
from a clean detached checkout using the committed lockfile and Git dependency,
without the local Storj checkout. Before publishing crates, select a crates.io
version containing the retry API and update dependency requirements; a workspace
patch does not carry into downstream dependency resolution.

The early upload-budget check compares each target pack size, capped by that
blob type's estimated retained repack bytes, to the configured upload budget.
It is deliberately a planning check, not an exact output-size calculation.
Recompression, the final blob, and headers can change the completed size, so
the existing runtime guard remains. Validation happens before prune mutations;
a preceding CLI `forget` is a separate operation.

Tree-loader caps use the backend's advertised connection limit, independently
of the opt-in repack worker setting. They do not create a global semaphore for
all commands, other phases, or other clients.

No chunk-index algorithm change was made. Bounded allocation currently trades
additional per-blob lookup work for avoiding very large contiguous allocations;
changing that requires its own profiling and benchmarks.

## Validation

- 206 core unit tests passed (205 in the full run, plus the added cold/warm
  cache limit test).
- 53 backend tests passed; one ignored test was not run.
- 13 prune pipeline tests passed; the ignored benchmark was not run.
- 24 existing prune integration cases passed.
- Clippy passed for core/backend libraries and tests with `-D warnings`,
  **without** the earlier `manual_clamp` exemption.
- Clean-checkout default backend build passed with `--offline --locked` after
  fetching the pinned Git dependency into Cargo's cache.
- Formatting and diff checks passed.

The known snapshot drift described by the second reviewer was not changed.
The full integration suite was not rerun; the targeted prune cases above were.
No performance rerun or remote deployment was part of this review follow-up.
Validation logs are retained in `target/external-review-followups/`.
