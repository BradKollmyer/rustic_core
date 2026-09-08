# Furies S3 prune stack profile

The six throughput trials did not collect stack profiles. This separate diagnostic
ran on the Darwin workstation on 2026-09-08 using `/usr/bin/sample`; Linux `perf`
is not installed on this host. It uses the same frozen executable as the earlier
benchmark (production core `5ef673a`, harness `4fd68db`, optimized test profile).

## Capture

A fresh 4 GiB fixture used 128 MiB packs, five connections, a 128 MiB read budget,
a 1 GiB upload budget, fast repack and a 1,024-descriptor soft limit. Three
five-second, nominal 1 ms stack captures started 5.16, 25.46 and 45.23 seconds into
prune. All three completed successfully within the 96.82-second prune, before
its subsequent full data check. These timings are diagnostic, not additional
samples for the earlier five-versus-ten comparison.

The run read 3,867.24 MiB of pack ranges and wrote 2,048.21 MiB. Full integrity and
publication-order checks passed. No retries or EMFILE errors occurred. The one
absent-local-pack cache-removal warning was the same ENOENT observed in the
throughput trials. Sampled peak RSS was 1,483 MiB and peak descriptors were 17.
The generated S3 prefix was deleted and verified empty; local fixture/key cleanup
also completed.

## Findings

Across the five `prune-pack` threads and three captures, 56,930 sampled thread
states partition as follows. These are inclusive **upload-worker observations**,
not CPU utilization percentages or an estimate covering the entire process.

| Worker state | Observations | Share |
|---|---:|---:|
| Waiting in OpenDAL backend writes | 43,909 | 77.13% |
| Waiting for a job on the upload queue | 12,193 | 21.42% |
| Waiting for a shared I/O permit | 709 | 1.25% |
| Hashing/copying pack bytes | 119 | 0.21% |

The backend-write stacks are `FileWriterHandle::process -> OpenDALBackend::write_bytes
-> blocking::Operator::write_options -> Tokio block_on/park -> pthread_cond_wait`.
They show multiple concurrent writes awaiting completion. Hashing observations
include SHA-256 and the small reader copies, not a hot packer membership scan.

The permit waits in these upload-worker stacks use `ByteBudget::acquire`, but
that primitive also implements `IoBudget::acquire(1)`. Their caller is the
upload worker's shared connection budget, so they must not be described as
proof of an undersized byte-buffer setting. Read workers also appear waiting
inside OpenDAL `read_options` and the prune budgets.

The early capture has 49.5% of upload-worker observations in backend writes;
the two later captures have 89.5% and 89.7%. Most of the remaining early states
are waiting for packs to arrive. This is a pipeline waiting on I/O, rather than
CPU saturation in the packer. `ps` process-lifetime CPU samples had a median
of 5.5% and maximum 39.3% of one core; those are not interval CPU measurements.

The retained `nettop` trace contains 64 complete nominal one-second process
counter records. Median counter increments were 55.6 MiB incoming and
25.2 MiB outgoing per interval; peaks were 124.4 and 31.3 MiB. This is a partial
trace, not a whole-run bandwidth measurement: terminating the original infinite
nettop process discarded its final stdio buffer. The three stack windows fall
within the retained portion. A later standalone trace arrived after the test
process exited and is excluded.

## Interpretation and next measurement

The useful finding is backend I/O wait with low client CPU demand. The profiles
do not separate network bandwidth/latency from RGW or OSD latency, nor do they
prove that increasing queues or worker counts will help. The repeated benchmark
already found no consistent throughput gain at ten connections on this path.
No production optimization is justified by these captures alone.

The next isolating measurement would compare direct S3 GET and PUT throughput
(single and concurrent requests) while sampling RGW and OSD latency. That can
separate the client/network path from storage-side delays before another code
change. Arc's native B2 path remains a separate workload and network path.

## Reproduction and artifacts

The runner now accepts explicit connection counts and opt-in profiling:

```sh
uv run --offline --no-project --with boto3 python scripts/benchmark-prune-s3.py \
  --binary target/furies-s3-build/prune_pipeline \
  --size-mib 4096 --repeats 1 --connections 5 --profile \
  --output target/furies-s3-new-profile
```

Network collection now uses finite six-sample segments, allowing nettop to
exit normally and flush its output. Segment launch offsets and prune-end time
are recorded; discard rows after prune end, which can include the subsequent
check. A separate 64 MiB S3 smoke test verified the new single-case path, complete
six-record network output, full data check and cleanup. It was too short to
capture stacks; the three successful 4 GiB captures validate stack collection.

Artifacts are in `target/furies-s3-profile-4g/`: raw stack profiles, capture windows,
partial network counters, metrics, `analysis.json` with per-thread counts, results
and cleanup confirmation. `target/furies-s3-profiler-smoke/` contains validation
of the finite network collector. Credentials are absent from both artifact trees.
