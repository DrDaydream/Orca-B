# Local dependency wait removal experiment — 2026-11-20

This revision is based directly on `d47cedd565376be499bf2f6a5e9b24bbcec290d1` on `adversary-experiments`. It removes local dependency availability as a prerequisite from proposal processing through final output.

## Changed behavior

- A complete authenticated proposal can trigger ECHO without loading its strong, weak or virtual dependencies. ECHO quorum / READY relay can deliver READY, and READY quorum can deliver FINISH, while those dependencies remain absent or unfinished locally.
- Completed blocks count toward the round's certificate quorum immediately. There is no second parent-availability check or pending round-advance queue.
- A locally finished (grade-two) block can enter Dag before its strong/weak dependencies. Grade one alone still does not authorize ordinary Dag admission.
- A commit-ready leader with a resolved predecessor can output the currently available strong/weak subgraph. Missing causal ancestors do not block output. Virtual edges remain evidence metadata and are not pulled into output.

Late-data indexes remain to refresh evidence or leaders that are still pending for predecessor order. They are not dependency availability barriers.

## Quorum and validation

ECHO and READY weights come from distinct authenticated voting authorities for the **same proposal ID, round and proposer**. Contributions may arrive from different nodes, in any order, and in separate messages. A signer contributes its committee stake once per phase; duplicate votes and votes for another proposal cannot supply the target quorum. The ECHO, READY relay, READY finish and per-round certificate quorum thresholds are unchanged.

The existing wire protocol sends signed votes. A wire vote batch groups votes from one sender/phase/round for transport; it is not a restriction that a QC must have a single signer. The accumulator combines votes across batches and peers. The Rust `Certificate` type remains a local delivery record, not a newly introduced portable aggregate-QC format.

Proposal authentication, own-payload hash/completeness/storage, edge-set disjointness, authority membership, one local vote per proposer/round, vote signature and instance binding, and leader decision/predecessor ordering remain in force. Own proposal data and an actual leader record are still needed to process or output that object.

Parent-derived validation previously ran after loading every parent: parent-author uniqueness/stake quorum, strong-parent previous-round checks, weak-edge age checks and virtual-target previous-round checks. That metadata-dependent voting gate is removed too; it cannot be guaranteed for absent parents. This does not remove the signed-vote quorum thresholds.

## Interpretation of results

This is an experimental change to protocol semantics, not a consensus-safety-preserving latency optimization. An unavailable ancestor is omitted from that output sequence, not reconstructed from its digest. A committed leader is not replayed solely because its missing ancestor later arrives; later inclusion depends on subsequent references and the variant's existing ordering/GC rules. Nodes with different local data can emit different sequences or omit transactions.

Use this revision to measure the effect of dependency waits. A lower E2E value among completed samples does not establish equal transaction coverage or agreement; compare completed/sent samples, output sequences and throughput alongside E2E latency. No EC2 performance improvement is asserted by this change. The later gRPC-stream and metric revisions above the requested base are not included.

## Validation commands

```bash
cargo test --locked --workspace -- --test-threads=1
cargo test --locked -p primary -p consensus --features benchmark -- --test-threads=1
```

Regression tests exercise missing strong/weak/virtual data through GRBC, different signers and duplicate votes, completed-quorum round advancement, grade-two admission before dependencies, and final output with absent history. Existing signature, payload, voting, protocol-support and predecessor-order tests remain part of the suite.
