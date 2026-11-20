# A1 runtime and common-clock changes (2026-11-21)

## Protocol behavior

This experiment builds on NO-DEPENDENCY-WAIT.md; it does not restore the removed
local causal-dependency checks or establish a new general safety proof.

At the user's request, a node delivers its own proposal locally after complete
signature/payload validation and successful atomic local storage. Core publishes
GradeOne and GradeTwo without waiting for ECHO/READY votes for that self proposal.
Only one self proposal per round can use this path. A saturated peer broadcast
queue or outbound vote queue does not gate this local delivery. The existing
round-advance threshold still counts distinct proposal authors.

Local delivery is tracked separately from network FINISH. The author continues
normal ECHO and READY participation so remote replicas can complete. Early final
commit feedback preserves required own votes, including buffered authenticated
votes before Core has consumed the stored-header callback. Invalid proposals and
failed storage never use the immediate-delivery path. Duplicate feedback is
idempotent, and the new caches follow existing garbage collection.

For remote proposals, ECHO -> READY -> FINISH and distinct signed voter thresholds
remain unchanged. There is no extra echo-quorum delivery mode. Final ordering is
unchanged. Local delivery is not final transaction commit and is never the E2E
endpoint.

## Runtime

The serialized RocksDB actor uses a Tokio blocking worker, so synchronous reads
and writes do not occupy asynchronous protocol workers. FIFO writes, atomic
immutable batches and post-write notifications retain their existing semantics.
Successful atomic storage uses the existing RocksDB options, not a new fsync
guarantee. TCP_NODELAY is enabled on accepted and outgoing protocol connections
and the client input connection. Local payload validation/storage and broadcast
can progress concurrently with bounded producer backpressure.

Outbound vote capacity is serviced independently by Core. An unsent vote for
one proposal cannot gate an already-qualified remote proposal's local grades.
Deferred proposal retries retain the scheduler's original release decision.
Committed feedback publishes a monotone cleanup round without waiting for the
worker network; workers receive the latest round when their sender can proceed.

Orca-B's consensus feedback router also keeps ABA broadcast overflow in a disk
FIFO. It continues forwarding commit feedback while outbound ABA capacity is
unavailable, and replays every batch in order through the existing signing,
scheduling and acknowledgement path. Resident overflow buffers are bounded;
disk consumption grows with the unsent backlog. Disk I/O can still take time,
and storage failures remain fatal. The spool is temporary process state, not
new crash-recovery state.

## Measurement

New local and remote runs default to a schema-2 common UTC clock. Clients connect
before readiness; the controller distributes the start plan 30 seconds ahead and
requires matching acknowledgements at least 5 seconds before load starts. The
remote runner checks chrony synchronization. A start more than 500 ms late is
invalid. The UTC plan is saved with raw logs and the parameter snapshot.

`warmup` precedes the fixed measurement interval; `drain` follows it. Generic
`fab local`/`fab remote` defaults remain zero for both; the separate acceptance
runner explicitly selects warmup=10s, measurement=60s, drain=10s. A1 crash nodes
remain absent throughout, including warmup. Each surviving client retains its
configured share of total nominal input rate.

E2E always starts at the original client transaction-release timestamp and ends
at final ordered commit. The common clock changes sample selection, not this
latency subtraction. The main commit window selects final commits in
[start_us,end_us), retaining the full latency of samples released during warmup.
Its mean is the unweighted mean of all active replicas' sample means; a missing
or empty replica cannot silently produce an acceptance pass. Throughput uses
final committed payload bytes / transaction size / fixed measurement duration.

The companion release cohort selects releases in [start_us,end_us) and observes
them until the exclusive drain_end_us cutoff. Its mean, unfinished samples,
completion coverage and TPS remain visible. Each client stop record gives the
total release count, which must match a contiguous sequence of sample IDs.
Clocked parsing requires an explicit microsecond final timestamp and a single
worker record with payload bytes and the complete sample ID list for every
batch finalized before the drain cutoff. Replica-summed sample counts are not
unique transactions. Old logs without a clock plan retain the explicitly
labelled legacy calculation.

The controller starts its final process-health poll at or after the drain
deadline. It collects available logs after stopping its own processes, including
on failed runs. The remote runner removes its RocksDB stores and uploaded keys
after verifying process exit. If collection is incomplete, the run's logs
remain on the testbed and the controller records their location and missing
files in collection-issues.json.

Process health checks distinguish ERROR-level records, fatal exits and panics
from ordinary WARN connection retries to intentionally absent A1 nodes. These
warnings remain in the raw logs and do not change timestamp-based E2E arithmetic.

## Verification and EC2 limits

Regression tests cover delayed local-store acknowledgements, blocked peer/vote
queues, duplicate and invalid proposals, normal subsequent READY votes, early
commit feedback, database worker responsiveness, common-clock arming, exact
measurement boundaries, incomplete coverage and archive integrity.

See benchmark/A1_ACCEPTANCE.md for the separate 30-case acceptance matrix and
commands. Local simulated-network results do not establish EC2 performance.
The existing Mysticeti campaign must finish before installing or running Orca
on its hosts. The less-than-3-second EC2 goal remains unverified until the saved
results pass all agreed cases. The currently running figure toolkit is not
modified by this change.
