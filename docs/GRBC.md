# Orca-B complete-proposal ECHO/READY broadcast

This branch replaces the old cross-authority worker Batch/ACK quorum and
primary vote/certificate dissemination with one complete-proposal protocol.

## Components and flow

1. **Local worker:** receives transactions, seals and stores a batch, and sends
   the serialized batch to its own primary. The local transport ACK only
   confirms handoff; it has no GRBC voting weight. The normal batching path
   does not broadcast to peer workers or spawn `QuorumWaiter`.
2. **Proposer primary:** selects batches, fixes the author, round, payload
   digest/worker-id map, strong parents, weak edges and virtual edges, and
   signs that header. A dedicated proposal task loads the bytes, validates and
   stores the complete proposal, then broadcasts `Proposal`. This
   is the first cross-authority data transmission.
3. **Every primary:** verifies the signed header and the exact payload digest
   map, retains the complete data, and checks the required DAG dependencies.
   It broadcasts one signed `ECHO` for each `(origin, round)` instance.
4. **Local READY:** on an ECHO quorum, or on the READY relay threshold, the
   primary records its own READY vote, delivers local grade 1, and broadcasts
   READY. It does not wait for an incoming READY when ECHOs already suffice.
5. **Local FINISH:** on a READY quorum, the primary stores a local completion
   record and delivers grade 2. ECHO votes alone cannot produce FINISH.

The thresholds use the existing stake-based committee functions. For equal
stake and `n = 3f + 1`, quorum is `2f + 1` and the READY relay threshold is
`f + 1`. Each phase has its own distinct-sender set. Local votes count once.
Each honest process sends at most one ECHO and one READY per instance.

## Messages and storage

- `Proposal` carries the complete header and all referenced serialized batches.
- `GrbcVote` binds phase, header ID, proposer, round and voter in its signature.
- `GrbcVoteBatch` only bundles votes created by the same sender in one phase
  and round, potentially for several proposals. It is not a quorum proof.
- Neither ECHO nor READY transmits an aggregated certificate. There is no
  `PrimaryMessage::Certificate` path.
- The legacy Rust type `Certificate` remains a **local** consensus/storage
  delivery record. Production grade records have an empty `votes` vector.
- Recovery returns a complete proposal plus the responding node's own votes.
  The requester must assemble its own local counts; it cannot import a
  remote FINISH assertion. Votes received before data are buffered and cause
  proposal requests, with retries.
- Grade-1 records use `orca-ready-v2/`; payloads and local votes also have
  separate `v2` key prefixes. Strong/weak dependency checks
  before ECHO require local FINISH records. Virtual targets require their
  complete-proposal signed header markers to check rounds, not a local READY grade: another
  node's READY does not guarantee this node can immediately reach READY.

### Content-addressed proposal storage

The primary stores each batch once under its digest. It stores only the signed
header under `orca-proposal-header-v3/<block digest>`; it no longer persists a
second copy of the batches inside a serialized proposal. Core retains headers,
rather than full proposal payloads, in its recent-proposal index.

After full proposal verification, missing batch entries and the header marker
are inserted in one RocksDB WriteBatch. Existing entries are not overwritten,
including batches already handed off by the local worker. The store serializes
existence checks and writes, reports write errors, and notifies waiters only
after the entire write succeeds. It uses the existing default RocksDB durability
options, without adding an fsync. A header marker means complete authenticated
data was retained; it does not imply DAG validation, READY, or FINISH.

Virtual-edge dependency checks and waiters read only this small header record.
Strong/weak dependencies continue to read the small local FINISH records. Both
ordinary repair replies and leader recovery reconstruct the full wire Proposal
from its header and batch keys; missing data produces an error instead of a
partial reply. Recovery still includes only the responder's own votes.

This deduplication is within each primary's store. The local worker's batching
store remains separate. Network Proposal/ECHO/READY formats and quorum thresholds
are unchanged. Legacy `orca-proposal-v2/` records are not migrated; run with fresh
experiment stores (`fab local` already resets them). This change does not by
itself establish a measured throughput or latency improvement.

### Validation reuse and independent vote transport

Each incoming complete proposal passes header authentication, size/shape checks
and all batch digest checks once in a dedicated proposal processor. Core receives
an opaque stored-header result only after successful validation and atomic
storage. Local proposals use the same validation path, without looping back
through a second full verification. New network arrivals are still validated;
matching an existing header ID never authenticates new payload bytes.

Incoming votes are verified once and wrapped in an immutable authenticated type.
The buffer and aggregator accept this type, so releasing buffered votes does not
repeat signature verification. Locally signed votes enter directly. Counting
still checks header ID, round, origin and distinct sender/phase. Duplicate votes
do not rerun grade evaluation. Local FINISH records must match the entire cached
authenticated header, including its body and signature, and satisfy the local
dependency and READY-quorum checks; they no longer repeat header verification.

ECHO and READY use a dedicated bounded queue, sender task and TCP connection per
peer, separate from complete proposals and repair payloads. Both phases share
the small-message transport; they are separate from bulk data, not separate
from each other. READY is enqueued immediately after local grade-1 delivery.
The sender coalesces only already queued votes from the same sender, round and
phase, without a batching delay or waiting for another Core iteration. Repair
votes use this sender too; Helper queues them before assembling the payload.
Reliable send handles are retained until ACK or round GC.

Receive-side proposal queues are separate from the vote/control queue. Local
and remote proposal processing use separate consumers; expensive validation,
hashing and serialization run outside Core on blocking tasks, with at most one
validation job per consumer. Large network frames are also decoded outside the
async runtime threads. Leader repair now delegates bulk data work to Helper.
No listening ports, wire message formats or `fab local` settings are added.
CPU, disk and physical network bandwidth remain shared, so this removes queue
and connection contention with payloads without guaranteeing latency or TPS.

The worker and primary remain separate executables. Their cross-authority
dissemination responsibilities are combined into the primary's GRBC path;
this change does not combine their processes. Legacy worker sync/helper and
unused quorum-waiter source remain, but are not in the normal proposal path.
Remote transaction bytes now reside in primary storage, not remote worker
replicas. An application that fetches executed batches from worker replicas
must adapt its data retrieval path. The repository's sample node only logs
consensus output.

## Bounds and compatibility

- Serialized batch data per proposal is limited to 4 MiB. The complete
  serialized proposal is limited to 7 MiB, leaving room for its message tag
  below the transport's 8 MiB frame limit.
- The proposer retains batches that do not fit for subsequent rounds; the
  worker splits batches before the data limit. A single oversized client
  transaction is rejected with a warning.
- All nodes and workers must be rebuilt together. The new wire enums and
  storage interpretation are incompatible with old binaries/stores. Use
  fresh experimental stores; no migration is included.
- This remains an in-process experimental implementation. Voting locks and
  counters are not restored after crashes. The existing round GC limits
  arbitrarily delayed recovery, and storage has the existing write/error
  semantics. This change does not establish crash-recovery safety, full
  Byzantine hardening, or a formal proof of the surrounding Orca protocol.

## Consensus integration repair

A real late-start-node test exposed a pre-existing assumption in the fast
commit path: forced DAG admission could be mistaken for complete causal
history. A node could commit before missing strong/weak ancestors arrived.
The commit queue now waits for all tracked strong/weak history of that leader.
History traversal also follows already-forced but not yet committed parents.
Virtual edges are still excluded from final causal ordering. A regression
test covers an ancestor missing behind an already-forced parent.

After ABA decides 1, the experiment intentionally accepts a complete,
authenticated observed leader before local READY. It traverses strong/weak
history and waits for missing data, without adding a READY gate or a new
equivocation detector. `Observed` therefore precedes DAG dependency validation;
that validation still gates the local ECHO/READY/FINISH state machine. Traversal
orders the chosen history; it does not by itself disambiguate two conflicting
signed proposals from the same author and round. End-to-end experiments here
assume no such equivocation. Consensus promotion does not fabricate primary
READY votes or bypass the primary's FINISH quorum.

The second audit fixed deferred weak/virtual round checks (including genesis
weak edges), mixed dependency wakeups, stale repair requests caused by
metadata-inconsistent votes, and validation of local completion records.
The proposer also stops polling an already elapsed timer while awaiting
parents. Regression tests cover early ABA acceptance, late ungraded leader
recovery, missing causal history, and the separate GRBC voting gates.

## Reproduce validation

```sh
cargo test -p crypto -p store -p primary -p worker -p consensus -- --test-threads=1
cargo build -p node --features benchmark
python3 scripts/grbc_smoke.py --scenario all
python3 scripts/grbc_smoke.py --scenario silent
python3 scripts/grbc_smoke.py --scenario late
```

`grbc_smoke.py` launches real primary/worker processes with fresh RocksDB
stores and nonempty client transactions. It verifies sustained payload
commits, recent progress and identical commit prefixes, keeps its logs, and stops only its
own child processes. `silent` omits one of four authorities; `late` starts
the fourth authority after three seconds. These are functional checks, not
throughput/latency benchmarks or exhaustive asynchronous fault simulation.

Validation on 2026-11-12 used Rust 1.85.0, MSVC and the real RocksDB backend
on Windows. The repository's Linux-only forced compiler settings in
`.cargo/config.toml` were overridden only in the local build command.
Ubuntu execution has not been performed in this session. Existing warnings
about deprecated dependencies and unused legacy code remain.

The storage optimization run passes 72 unit tests: primary 32, store 5,
worker 9, consensus 26, plus the doctest checks. Tests cover shared batches,
atomic header publication, metadata-only dependency reads, and full proposal
plus individual-vote recovery before READY. The node builds with the benchmark
feature. Real-process checks pass for all nodes online (706 common blocks),
one absent node (538 common blocks), and a late node (1047 common blocks,
7 rounds behind at shutdown with sustained progress). These functional checks
do not measure performance or establish adversarial safety.

The validation/transport optimization passes 82 unit tests: crypto 7, primary
35, store 5, worker 9 and consensus 26, plus doctest checks. New regressions
cover forged votes before proposal arrival, changed payload bytes carrying a
known header, and ECHO/READY delivery while a 512 KiB proposal is blocked on a
full bulk queue. Recovery checks require data and votes on separate connections.
The benchmark-feature node builds. Real-process checks pass with 709 common
blocks for all nodes online, 547 with one absent node, and 1166 with a node
starting three seconds late (20-second run, all nodes at round 321 at shutdown).
These checks use the same Windows/Rust/RocksDB environment described above;
Ubuntu 24.04 throughput and latency remain unmeasured.
