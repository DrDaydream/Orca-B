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
6. **Actual local commit:** a committed block with matching complete local data
   also becomes a completed dependency and counts toward round advancement.
   This shortcut emits no GRBC grade or additional vote; see below.

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
  The requester assembles its own local counts unless local consensus has
  actually committed the block; it cannot import a remote FINISH assertion.
  Votes received before data are buffered; missing
  proposals are requested with retries, subject to the in-flight grace below.
- Grade-1 state is kept in memory; the unused `orca-ready-v2/` record and
  raw header-ID copy are no longer written. Payloads and local votes retain
  their separate `v2` key prefixes. Strong/weak dependency checks
  before ECHO require local completion, from FINISH or actual commit. Virtual targets require their
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
Strong/weak dependencies read the small local completion records. Both
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
READY votes. Actual commit can now satisfy local dependencies and count toward
round advancement without collecting a FINISH quorum, as described below.

The second audit fixed deferred weak/virtual round checks (including genesis
weak edges), mixed dependency wakeups, stale repair requests caused by
metadata-inconsistent votes, and validation of local completion records.
The proposer also stops polling an already elapsed timer while awaiting
parents. Regression tests cover early ABA acceptance, late ungraded leader
recovery, missing causal history, and the separate GRBC voting gates.

## GRBC vote processing

Core caches the fixed committee's quorum and READY relay thresholds. Counting
a vote borrows the stored header; it clones the complete header and evaluates
GRBC only when fresh weight crosses the ECHO quorum, READY relay threshold, or
READY quorum. Duplicate votes still contribute no weight. Authentication and
instance matching are unchanged, as are the wire messages and send batching.

Proposal and dependency arrival still evaluate accumulated votes explicitly,
so thresholds reached before validation are not lost. A locally generated
READY is counted before checking FINISH, allowing the same update to complete
the instance. Regression tests cover delayed proposals/dependencies, late and
duplicate votes, weighted thresholds and single-authority completion.

Validation of this change passes all 91 workspace unit tests with all features,
the doctest checks and the benchmark-feature node build on Windows. The
four-node process check has an identical commit sequence at all replicas.
The ten-node check retains a common commit prefix but shows substantial
replica lag; a comparison binary built from the unchanged parent revision,
using the same authority keys, also stalls and fails the recent-progress check.
These runs do not establish a throughput or latency improvement. Ubuntu 24.04
performance remains to be measured.

## Proposal registration and bounded Core work

Decoded remote proposals are tracked before waiting for space in the bulk
queue. The same guard follows validation, storage and the stored-header queue
until Core consumes the result. Local proposals are tracked before payload
assembly through broadcast and Core registration. Tracking stores only a
block digest and a reference count, never another payload copy. A failed or
cancelled operation, a stale proposal or a dropped queue releases its guard;
one failed duplicate does not clear another copy still being processed.

An authenticated vote for an unknown header still enters the pending-vote
buffer. If the corresponding proposal is in flight, Core defers the immediate
repair and periodic retries. Without matching work, the first request remains
immediate. Suppression expires two seconds after the first authenticated vote;
the existing one-second repair tick then retries even if processing is stuck.
Additional arrivals or votes do not reset that deadline. Tracking is only a
scheduling hint: every proposal still passes full validation before Core can
observe it or vote. Frames still in transit or being decoded are not tracked.

Core uses a 32-work-item budget, reduced from 256. Each vote
costs one item; opening a vector and other control messages also cost one.
Core services one consensus command and one stored proposal at the start of
each burst, so actual commit feedback is not starved by hot vote traffic.
Oversized vectors retain their unconsumed votes across bursts, without new
network packets; later control frames do not overtake that remainder. At the
end of a full burst Core yields to the runtime and reselects among its inputs.
An empty network queue returns directly to selection without an extra yield.
Queue capacities, outgoing vote batching and GRBC thresholds are unchanged.
The smaller budget returns control to the input selector more often, including
when a vote vector is larger than one burst.

The initial 32-item change passed all 46 primary unit tests with all features, doctest
checks and the benchmark-feature node build on Windows. The process results
below were recorded with the earlier 256-item setting; performance with 32
items remains to be measured.

The original 256-item change passed all 96 workspace unit tests with all
features and the doctest checks on Windows. Regressions cover processing and stored-header queues, failure and
duplicate guards, queue cancellation, a repair deadline that cannot be renewed,
oversized-vector continuation/FIFO, and a proposal completing during a burst.
The benchmark-feature node build also passes. Final real-process runs agree on
698 blocks with four nodes online and 1185 blocks with the fourth starting
three seconds late; both end with zero round lag. The ten-node run keeps a
516-block common prefix and recent progress at every node, but still has a
53-round maximum lag. These Windows debug runs verify functional behavior;
they do not establish Ubuntu throughput or latency gains, or resolve the
previously observed ten-node lag.

## Completing committed proposals without vote repair

The existing local consensus output carries the actual committed sequence,
including the leader and the strong/weak history committed with it. Core now
applies this sequence before forwarding its cleanup batch to the garbage
collector. A remote commit claim, an ABA decision alone, or an uncommitted
proposal is not a trigger for this shortcut.

Each committed header must exactly match its complete authenticated local
proposal. Core uses its header cache, falling back to the atomic stored-header
marker when the result has not yet been registered. This reads only metadata;
it does not reload batches or repeat payload hashing or signature verification.
Missing local data or a mismatched header cannot create a completion record.

Core stores a local completion record and closes the author's voting slot,
without synthesizing ECHO/READY votes or emitting GradeOne/GradeTwo events.
It publishes the entire committed sequence before counting new completions
toward the ordinary distinct-author round quorum. Records completed earlier
by GRBC are not written or counted again. Strong/weak dependency waiters wake
from these records; corresponding repair requests and buffered votes are
cleared. The garbage collector receives one batch after this work completes.

Both GRBC FINISH and actual commit are terminal for local broadcast processing.
Late votes are discarded before verification, without applying their untrusted
fields; stored-result duplicates and dependency loopbacks also return early.
Existing signed local votes remain available for peer recovery. This changes
the meaning of the local completion record: it proves dependency usability in
this experimental implementation, not necessarily a locally collected READY
quorum. Ordinary uncommitted instances retain their ECHO/READY thresholds.

Validation passes all 100 workspace unit tests with all features, doctest checks
and the benchmark-feature node build. New regressions cover stored but not yet
registered proposals, missing or mismatched local data, dependency release and
idempotent round counting, and commit feedback during a hot vote burst. Late
votes and dependency loopbacks after FINISH are checked to do no further work.
Windows debug process checks have identical commit prefixes: 697 blocks with
four nodes online, 544 with one absent authority, and 1170 with a node starting
three seconds late; all three end with zero round lag. Ten nodes retain a
792-block common prefix and recent progress at every node, but still have a
30-round maximum lag. These functional runs do not establish Ubuntu latency or
throughput improvements, and the ten-node lag remains unresolved.

## Simplified processing

Rules 1 and 2 no longer pre-admit causal history before offering their ABA
input. The common commit entry traverses known strong/weak history once, and
the existing commit queue still waits for missing ancestors and preceding
leaders. ABA commit and late-leader recovery use the same entry. Rule thresholds,
grade-2 promotion for rule 2's third condition, and distinct-author counting
remain intact.

Rule 2 counts borrowed Dag entries using the existing strong-support index and
two-hop virtual check. Path classification no longer clones a whole Dag round,
allocates a result vector, or creates temporary OS threads. Leader selection
uses the committee's already ordered keys. Unused legacy traversal and
CertificateWaiter code, channels, and old worker-payload checks are removed.

Core uses its complete-proposal index for first-observation notification and
GC, eliminating three redundant state tables. Round retries run when completion
records become available. Core and HeaderWaiter collect state only when the GC
boundary advances; late commits still publish old dependency records without
reviving old voting state. The 32-item burst services pending commands and
stored proposals once at its start. All 101 workspace unit tests with all
features and doctest checks pass, as does the benchmark-feature node build on
Windows. Regressions include late commits after GC, retired record writes, and
combined strong/virtual support counted once.

Windows debug process checks retain identical commit prefixes: 689 blocks for
four nodes online, 541 with one absent authority, and 1176 with a late node;
these runs end with zero round lag. Ten nodes retain a 1063-block common prefix
and recent progress at every node, with a four-round maximum lag at shutdown.
These are functional checks; Ubuntu throughput and latency remain unmeasured.

## Bounded strong-path indexing

Rule 2 and the r+2 ABA input query strong paths from round r+2 to round r.
Validated strong edges go back exactly one round, so the support index now
stores only two-hop ancestors. It no longer copies the full transitive history
into every new block. A late intermediate updates its immediate children,
including children already admitted to Dag. Commit-history traversal and
distinct-author support counting retain their existing semantics.

The regression suite passes 103 workspace unit tests with all features; a
separate manual index probe is ignored by default. Five alternating Windows
release measurements on a synthetic 10-authority, 80-round graph reduced the
median observe/promote time from 171.25 ms to 8.14 ms. Each support map contained
780 keys instead of 31,600. This measures index overhead only, not throughput
or end-to-end latency. Run the probe with:

```sh
cargo test -p consensus --release measure_strong_support_index -- --ignored --nocapture
```

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
