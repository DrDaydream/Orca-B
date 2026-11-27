# Orca-B

Orca-B is a research implementation of a DAG-based consensus protocol built on Narwhal. Workers broadcast transaction batches to peer workers. After a stake quorum acknowledges a batch, its author forwards the digest and worker ID to its primary. GRBC proposals and proposal-repair replies carry the signed header with these references. A receiver waits for its local workers to store any missing batches before it can ECHO. Pending batch recovery allows other available proposals to progress. Orca-B combines the Good, Deferred and Fallback leader rules with asynchronous binary agreement (ABA) for recovery. The protocol is written in Rust. The Python benchmark uses Fabric to run a whole committee on one machine.

Deploy matching primary and worker binaries throughout the committee. This digest proposal format changes the wire protocol. The commands below target Ubuntu 24.04 x86-64. The repository enables SSE4.2 and PCLMUL for this build. [Narwhal and Tusk paper](https://arxiv.org/abs/2105.11827)

## Quick start

### Install

Run these commands in Bash. The Rust build uses Clang 14 and its shared library to compile the bundled RocksDB. Cargo reads the compiler and library paths from `.cargo/config.toml`.

```bash
sudo apt-get update
sudo apt-get install -y \
  git curl ca-certificates build-essential cmake pkg-config libssl-dev \
  clang-14 libclang-14-dev python3 python3-venv

git clone --branch adversary-experiments --single-branch https://github.com/DrDaydream/Orca-B.git
cd Orca-B

if [ ! -x "$HOME/.cargo/bin/rustup" ] && ! command -v rustup >/dev/null; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.85.0
fi
export PATH="$HOME/.cargo/bin:$PATH"
rustup toolchain install 1.85.0 --profile minimal
rustup override set 1.85.0

python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r benchmark/requirements.txt

cd benchmark
fab --help local
```

The directory override selects Rust 1.85.0 for this checkout, matching the toolchain used by the benchmark build scripts. In a new shell, activate the virtual environment from the repository root with `source .venv/bin/activate`, then enter `benchmark/`.

### Run a local benchmark

The following commands run from `benchmark/`. The first run builds the release binaries with the `benchmark` feature. Each `fab local` invocation runs one experiment with one input rate. It replaces the previous `logs/` directory and local node databases, so copy any logs you want to keep before the next run.

```bash
# Four-node baseline with an explicit A0 label in the summary.
fab local --adversary=A0 --nodes=4 --workers=1 --rate=50000 --tx-size=512 --duration=20
```

To compare the other modes with the same workload, choose one of the following runs. All three use the local defaults of four nodes, one worker per node, 512-byte transactions and a 20-second measurement window.

```bash
# A1: authority 3 and its client and worker stay offline for the entire run.
fab local --adversary=A1 --byzantine-ids=3 --rate=50000

# A3: authority 0 is the fixed Byzantine sender.
fab local --adversary=A3 --byzantine-ids=0 --seed=7 --delay-ms=200 --rate=50000

# A5: network scheduling across senders with an empty fixed Byzantine set.
fab local --adversary=A5 --seed=7 --delay-ms=200 --rate=50000
```

### Workload and timing parameters

| Local option | Default | Meaning |
|---|---:|---|
| `--nodes` | 4 | Number of authorities in the full committee, including any authorities selected for A1. |
| `--workers` | 1 | Workers per authority. Each active worker has one load-generating client. |
| `--rate` | 50,000 tx/s | Input target for the full committee. Pass a single integer such as `50000`. |
| `--tx-size` | 512 B | Size of each client transaction in bytes. Minimum 9. |
| `--duration` | 20 s | Length of the measurement window. Compilation, startup, warmup and drain add to elapsed wall time. |
| `--warmup` | 0 s | Time to send input before the common measurement window opens. |
| `--drain` | 0 s | Extra time to observe final commits for previously released samples after input stops. The headline final-commit window ends at the original cutoff. |
| `--common-clock` | `true` | Coordinate all clients on one measurement window. Use `--common-clock=false` for legacy timing with zero warmup and drain. |
| `--header-size` | 1,000 B | Batch-digest bytes a primary aims to accumulate before proposing. The proposal timer can trigger earlier once the previous round's parent set is ready. |
| `--debug` | off | Increase primary and worker log verbosity for this run. |

The launcher assigns each client `ceil(rate / (nodes × workers))` tx/s. For four nodes, one worker each and `--rate=50000`, each client targets 12,500 tx/s. A0 starts four clients, for 50,000 tx/s in total. A1 with one crash starts three clients, for **37,500 tx/s**. The summary's `Input rate` sums the active client targets. Measured throughput appears under `RESULTS`.

### Fault and adversary parameters

| Mode | Local behavior |
|---|---|
| `A0` / `none` | Baseline with all authorities active and an empty Byzantine set. |
| `A1` / `silent` | Full-run crash-stop. The launcher omits the selected primaries, their workers and their clients. The committee keeps its original size and quorum. |
| `A3` / `targeted` | The fixed Byzantine senders delay selected leader-related GRBC messages within the configured round window. |
| `A5` / `network` | The network scheduler can delay selected leader-related messages from any sender. It also covers Orca-B's dedicated ABA channel. |

| Local option | Default | Meaning |
|---|---|---|
| `--adversary` | omitted | Select an explicit mode from the table. Bare `fab local` uses the legacy schedule with the configured `faults` value. Use `--adversary=A0` for a labeled baseline. |
| `--faults` | 0 | With the legacy schedule, controls the number of authorities selected per round while their processes stay online. Explicit A0/A1/A3/A5 runs require zero. |
| `--byzantine` | 0 | Select a count of fixed authorities. A1 takes the highest launch IDs. A3 and A5 take the lowest IDs. |
| `--byzantine-ids` | empty | Select specific zero-based launch IDs, e.g. `--byzantine-ids=1,3` in a seven-node committee. Use either this option or a positive `--byzantine` count. |
| `--seed` | 1 | Seed for the A3/A5 recipient selection. The round and authority keys also enter the selection. Each new local run generates fresh keys. |
| `--start-round` | 1 | First evidence round eligible for A3/A5 scheduling. |
| `--attack-rounds` | 0 | Number of eligible rounds starting at `--start-round`. Zero continues through the end of the run. A1 requires the default values for both round options. |
| `--delay-ms` | 200 ms | Delay applied to each message selected by A3/A5. |
| `--max-pending-messages` | 4,096 | Upper limit on the number of messages held by the delay scheduler. |
| `--max-pending-bytes` | 67,108,864 B | Upper limit on bytes held by the delay scheduler (64 MiB). |

The fixed set can contain up to `floor((nodes − 1) / 3)` distinct IDs. A1 and A3 require at least one. In A3 and A5 the scheduler uses the seed and round to select a subset of honest recipients for eligible messages.

For legacy runs that omit `--adversary`, the following environment variables control the original schedule.

| Variable | Default | Meaning |
|---|---|---|
| `ORCA_ADVERSARY_SEED` | `0` | Seed used to select authorities for each round. |
| `ORCA_RULE3_BEHAVIOR` | `mixed` | Behavior of a selected Rule 3 leader. Values are `mixed`, `silent` and `participate`. |
| `ORCA_CLIENT_DURING_SILENCE` | `send` | Keep sending during a scheduled silent slot, or set `pause` to pause client input. |
| `ORCA_CLIENT_SILENCE_SLOT_MS` | `max_header_delay` | Length in milliseconds of one slot in the client's wall-clock silence schedule. |

### Batching and synchronization

The `node_params` dictionary inside `local()` in `benchmark/fabfile.py` sets the following defaults. `header_size` also has the command-line override shown above.

| Setting | Default | Meaning |
|---|---:|---|
| `header_size` | 1,000 B | Proposal threshold measured in batch-digest bytes. |
| `max_header_delay` | 200 ms | Primary timer for proposing with the digests collected so far, once the round has its parents. |
| `gc_depth` | 50 rounds | Round depth used to discard stale synchronization state. |
| `sync_retry_delay` | 10,000 ms | Time before retrying a request for missing data. |
| `sync_retry_nodes` | 3 peers | Number of peers selected for a synchronization retry. |
| `batch_size` | 500,000 B | Worker target for transaction bytes per batch. |
| `max_batch_delay` | 200 ms | Worker timer for sealing a partial batch. |

## Read the results

The terminal prints `SUMMARY` with `CONFIG` and `RESULTS`. An explicit-mode run records the selected mode, such as `A3 (targeted)`, and the actual list of Byzantine launch IDs. Reprint the summary from the most recent local logs with:

```bash
fab logs
```

| Summary field | Interpretation |
|---|---|
| Consensus TPS / BPS | Committed batch bytes per second, measured from the first proposal to the last observed rule-ready commit. TPS converts bytes using the configured transaction size. |
| Consensus latency, P50, P99 | One observation per distinct committed batch, from its first proposal to the earliest rule-ready commit recorded by an included replica. |
| Leader and non-leader latency | Leader rule-ready time, non-leader final commit time, and non-leader rule-order time are reported separately from header creation. Leader commit interval measures the gap between successive leader commits. |
| Rule 1 / 2 / 3 ratios | Leader ratios count outcomes, including a separate Rule 3 skip category. Block ratios divide the blocks committed by each rule by the total committed blocks. |
| End-to-end TPS / BPS | In common-clock runs, final transactions or bytes per second in the measurement window, averaged across the included replicas. |
| End-to-end latency, P50, P99 | From the sampled client's release to ordered final commit at a replica. In common-clock runs the mean weights replica means equally. P50 and P99 pool all valid transaction–replica observations whose final commit falls in the window. |
| ABA average / maximum / minimum duration | Time in milliseconds for completed local ABA instances. |
| ABA mean / total send rate | Outgoing ABA traffic in B/s. The total sums measured send rates. The mean divides by the full committee size, including crashed nodes. Each rate is also divided by the transaction size to show its equivalent in tx/s. |

Raw logs and `comparison.json` are in `benchmark/logs/`. Explicit-mode runs also write a timestamped parameter snapshot under `benchmark/results/` with the configuration, active launch IDs and offered input rate.

The following excerpt comes from an archived 10-node A0 experiment at 50,000 tx/s, with 512-byte transactions and a 20-byte header threshold. Its timing uses the observed log span. Replaying the archived logs yields these values.

```text
 Adversary mode: A0 (none)
 Byzantine authority IDs: []
 Consensus latency: 1,107.616 ms
 Consensus latency P50: 1,051.000 ms
 Consensus latency P99: 1,949.000 ms
 ABA mean send rate per committee node: 15,553.826 B/s (30.379 tx/s equivalent)
 ABA total send rate: 155,538.257 B/s (303.786 tx/s equivalent)
 End-to-end latency: 1,493.179 ms
 End-to-end latency P50: 1,431.180 ms
 End-to-end latency P99: 3,226.602 ms
```

## License

This project is licensed under the [Apache License 2.0](https://github.com/DrDaydream/Orca-B/blob/adversary-experiments/LICENSE).
