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
   signs that header. Core loads all bytes and broadcasts `Proposal`. This
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
  remote FINISH assertion. Votes received before data are buffered; missing-data
  requests use a short arrival-order grace and bounded retries as described below.
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

### Batched votes and bounded proposal repair

The previous task-per-message transport optimization was reverted before this
revision. The implementation again uses Core's original bounded input bursts
(up to 32 messages) and outbox grouping by round/phase. There is no added
per-vote sender task, per-proposal processing queue or blocking-pool handoff.
Separate persistent sender instances keep complete proposals and small votes
on different TCP connections to the existing primary port. Helper also uses
separate data and vote connections. Independent transport does not isolate CPU
work: proposal validation and voting still share the Core execution loop.

For local proposals, Core assembles and verifies the full data once, enqueues
the serialized proposal for broadcast, then awaits the local atomic storage
operation. Transmission can overlap that write. Local ECHO still requires the
complete data to be stored and DAG dependencies to pass. Serialization borrows
the proposal wrapper instead of cloning all batch buffers.

A vote that overtakes data no longer triggers an immediate committee-wide
request. It creates one pending repair with a 50 ms initial grace. This grace
delays only a missing-data request, never delivery or voting for available data.
Decoded proposals are marked before entering Core's queue; a guard removes the
marker on completion, invalid data, or a dropped queue item. Queued/processing
data can defer repair, but not beyond a one-second bound from the first missing
vote. Repeated arrivals and votes cannot renew the bound indefinitely. Partial
network frames have no decoded header ID, so they are covered by the initial
grace rather than this marker.

If data is still absent, the first request goes to the authenticated voter that
announced it. After one second without data, retries fan out to the committee.
Receiving a valid full proposal cancels the pending repair. These timers are
recovery scheduling only; they do not weaken data validation or quorum gates.
This avoids normal arrival-order differences provoking many full-payload replies,
while retaining recovery when data is truly missing or the first peer is silent.

Validation reuse is retained without the extra tasks: incoming proposals and
votes are authenticated once per processing path; buffered votes keep an opaque
authenticated type. The aggregator still checks instance metadata and distinct
senders. FINISH compares the entire cached verified header and requires the
local READY quorum. New network payloads never bypass validation based only on
an existing header ID. ABA=1's early observed-proposal path is unchanged.

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
python3 scripts/grbc_smoke.py --nodes 10 --scenario all
```

`grbc_smoke.py` launches real primary/worker processes with fresh RocksDB
stores and nonempty client transactions. It verifies sustained payload
commits, recent progress and identical commit prefixes, keeps its logs, and stops only its
own child processes. The committee defaults to four nodes and supports
`--nodes 10`; `silent` omits the last authority and `late` starts it after
three seconds. These are functional checks, not
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

After the transport rollback and revision, 87 unit tests pass: primary 40,
crypto 7, store 5, worker 9 and consensus 26, plus doctest checks. Regressions
cover queued data suppressing repair, failed or repeated proposals not blocking
repair indefinitely, single-peer recovery followed by committee retry, and
forged votes. A full 512 KiB proposal reaches a peer before its local header
marker/ECHO; four votes per phase travel as two batched frames on a connection
separate from an unread data connection. The benchmark-feature node builds.

Real-process checks pass for four nodes online (703 common blocks), ten nodes
online (901 common blocks, maximum two rounds of lag at shutdown), one of four
nodes absent (547 common blocks), and one of four nodes three seconds late
(1182 common blocks, all at round 328 at shutdown). These are functional checks
in the Windows environment above, not measurements of Ubuntu 24.04 latency
or throughput at the user's 50,000 tx/s workload.
