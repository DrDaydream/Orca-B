# Optional proposal and GRBC latency trace

This branch adds diagnostics to `a309da2`. It is not a performance optimization.
The protocol, wire messages, parameters, default threads, CRC configuration and
serial AWS startup are unchanged. It does not include the TCP_NODELAY candidate.

Run the existing task with a primary-only trace:

```bash
cd /home/ubuntu/experiments/Orca-B-grbc/benchmark
ORCA_LATENCY_TRACE=1 fab remote
```

`ORCA_LATENCY_TRACE=1 fab local` also works with the existing local task.
The command generator forwards a fixed `RUST_LOG=info,orca_latency=debug` filter
to primary processes, including remote ones. Normal `fab local` / `fab remote`
uses the original command and logging behavior. No new tasks, queues, timers,
wire fields or protocol caches are introduced. Disabled trace macros do not
format messages; the receive clock and vote trace loop are gated by log level.
Setting `RUST_LOG=debug` manually also enables these logs, like other debug logs.

Trace rows use full header IDs (or batch IDs for batch events), round numbers,
and the existing benchmark logger's millisecond timestamps. Reuse the existing
`Header created`, `Created ... -> batch`, and commit records to join the path.

| Event or interval | What it measures |
| --- | --- |
| `batch_available` → `batch_queued` | Persisted local batch notification to proposer receipt |
| `batch_queued` → existing Header creation | Wait to include that batch in a proposal |
| `assemble_start` → `validate_start local=true` | Local payload reads and proposal assembly |
| `decoded`, `ack_and_decode_us` | Complete frame dispatch through ACK, decode scheduling and decode completion; excludes network reception |
| `decoded` → `validate_start local=false` | Remote proposal queue wait |
| `validate_start` → `validated` | Validation plus blocking-pool scheduling; local proposals also include serialization |
| `validated` → `stored` | Storage preparation, queue and write acknowledgement, with outbound enqueue running alongside for local proposals |
| `stored` → `registered` | Wait for Core to register the proposal; local path also waits for broadcast enqueue |
| `registered` → `parents_valid` | Core scheduling, buffered votes and DAG dependency resolution/checks |
| `parents_wait` | Missing dependency observation, which can repeat |
| `ready` | READY threshold crossed locally, before signing and GradeOne publication |
| `vote_queued` → `vote_dispatch` | Channel enqueue, sender waiting and grouping; ends before serialization/socket enqueue |
| `finish` | Locally completed after READY quorum and completion-store enqueue; before GradeTwo publication |
| `round_advance` | Parent quorum formed, before consensus/proposer notification |
| `payload_selected` | Selected bytes and remaining proposer digest count, before signing |

Do not interpret sender dispatch as bytes reaching the socket or a peer.
Do not interpret `ack_and_decode_us` as pure decoder CPU time. `Store::write`
only enqueues a write, while proposal storage awaits its write acknowledgement.
Use within-node deltas, account for clock adjustments, and exclude or explicitly
match repeated proposal copies; taking unrelated first/last records can invent
queueing delays. Small deltas can be zero at millisecond logger resolution.

Diagnostic logging has runtime and I/O cost when enabled. Use it to locate
waiting, then disable it for performance comparisons. Keep offered rate, node
placement and measurement window identical; report consensus/E2E latency,
final TPS and unfinished samples together. A lower ABA duration alone does
not establish lower consensus or end-to-end latency.
