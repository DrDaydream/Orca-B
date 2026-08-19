# Orca-B

[![Rust](https://github.com/DrDaydream/Orca-B/actions/workflows/rust.yml/badge.svg)](https://github.com/DrDaydream/Orca-B/actions/workflows/rust.yml)
[![Ubuntu](https://img.shields.io/badge/Ubuntu-24.04-E95420?style=flat-square&logo=ubuntu)](https://ubuntu.com/)
[![license](https://img.shields.io/badge/license-Apache--2.0-blue.svg?style=flat-square)](LICENSE)

This repository provides an experimental implementation of **Orca-B**, built on the [Narwhal and Tusk](https://arxiv.org/pdf/2105.11827.pdf) codebase. It retains Orca's graded reliable broadcast, VDag, strong and virtual references, and multi-rule leader commit path, while adding asynchronous binary agreement (ABA) for the slow path.

The project is designed for protocol research and benchmarking rather than production deployment. It uses real cryptography ([dalek](https://doc.dalek.rs/ed25519_dalek)), asynchronous networking ([Tokio](https://docs.rs/tokio)), and persistent storage ([RocksDB](https://rocksdb.org/)).

## Quick Start

The protocol is written in Rust. Benchmark orchestration and result parsing are written in Python and use [Fabric](https://www.fabfile.org/).

On Ubuntu 24.04, install the dependencies directly into the current user environment:

~~~bash
git clone https://github.com/DrDaydream/Orca-B.git
cd Orca-B

sudo apt-get update
sudo apt-get install -y \
  build-essential cmake clang-14 libclang-14-dev curl git tmux \
  python3 python3-pip

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"

python3 -m pip install --user --break-system-packages \
  -r benchmark/requirements.txt
export PATH="$HOME/.local/bin:$PATH"
~~~

Use Clang 14 for RocksDB's bindgen step:

~~~bash
export LIBCLANG_PATH=/usr/lib/llvm-14/lib
export CLANG_PATH=/usr/bin/clang-14
export CC=/usr/bin/clang-14
export CXX=/usr/bin/clang++-14
export CXXFLAGS='-include cstdint'
~~~

Configure the local experiment in `benchmark/fabfile.py`:

~~~python
bench_params = {
    'faults': 0,
    'nodes': 4,
    'workers': 1,
    'rate': 50_000,
    'tx_size': 512,
    'duration': 20,
}
~~~

The `faults` field enables dynamic adversarial scheduling while keeping all node processes online. Use configurations satisfying `nodes >= 3 * faults + 1`.

Run the benchmark:

~~~bash
cd benchmark
fab local
~~~

The first run recompiles the Rust workspace in release mode with the `benchmark` feature and may take several minutes.

### Local adversary options

For `fab local`, set the adversary count in `benchmark/fabfile.py`. Use `'faults': 0` for the baseline or a positive value for the following commands:

~~~bash
# Default reproducible mixed Rule-3 behavior; client traffic continues.
ORCA_ADVERSARY_SEED=42 \
ORCA_RULE3_BEHAVIOR=mixed \
ORCA_CLIENT_DURING_SILENCE=send \
fab local

# Pause client input according to the pre-generated silence schedule.
ORCA_ADVERSARY_SEED=42 \
ORCA_RULE3_BEHAVIOR=mixed \
ORCA_CLIENT_DURING_SILENCE=pause \
ORCA_CLIENT_SILENCE_SLOT_MS=200 \
fab local

# All adversarial Rule-3 leaders remain silent; ABA still runs.
ORCA_ADVERSARY_SEED=42 \
ORCA_RULE3_BEHAVIOR=silent \
ORCA_CLIENT_DURING_SILENCE=pause \
fab local

# Control case: adversarial Rule-3 leaders continue participating.
ORCA_ADVERSARY_SEED=42 \
ORCA_RULE3_BEHAVIOR=participate \
ORCA_CLIENT_DURING_SILENCE=send \
fab local
~~~

| Variable | Default | Meaning |
|---|---|---|
| `ORCA_ADVERSARY_SEED` | `0` | Deterministic per-round adversary seed |
| `ORCA_RULE3_BEHAVIOR` | `mixed` | `mixed`, `silent`, or `participate` |
| `ORCA_CLIENT_DURING_SILENCE` | `send` | Continue or pause client input |
| `ORCA_CLIENT_SILENCE_SLOT_MS` | `max_header_delay` | Wall-clock schedule slot in milliseconds |

With `faults > 0`, all nodes derive the same f adversarial authorities each round. An adversarial leader is routed to Rule 3, while non-adversarial Rule 1 and Rule 2 outcomes approach a 1:1 split over longer runs. ABA continues independently during adversarial silence. An ABA result produced before entering `r+3` is accounted for under Rule 2; later ABA results are accounted for under Rule 3.

In multi-server deployments ABA uses the committee's dedicated `aba_to_aba` address on TCP port 3005. Local ports are generated automatically.

### No-adversary baseline (`faults = 0`)

Set `'faults': 0` in `benchmark/fabfile.py` and run:

~~~bash
RUST_LOG=info fab local
~~~

The following output was produced by a 4-node, 50,000 tx/s, 20-second local run:

~~~text
-----------------------------------------
 SUMMARY:
-----------------------------------------
 + CONFIG:
 Faults: 0 node(s)
 Committee size: 4 node(s)
 Worker(s) per node: 1 worker(s)
 Collocate primary and workers: True
 Input rate: 50,000 tx/s
 Transaction size: 512 B
 Execution time: 20 s

 Header size: 1,000 B
 Max header delay: 200 ms
 GC depth: 50 round(s)
 Sync retry delay: 10,000 ms
 Sync retry nodes: 3 node(s)
 batch size: 500,000 B
 Max batch delay: 200 ms

 + RESULTS:
 Consensus TPS: 48,608 tx/s
 Consensus BPS: 24,887,270 B/s
 Consensus latency: 359 ms
 Leader commit latency: 184 ms
 Non-leader commit latency: 433 ms
 All committed headers latency: 381 ms
 Leader commit interval: 213 ms
 Non-leader rule-order latency: 409 ms
 Rule 1 leader ratio: 70.59%
 Rule 2 leader ratio: 4.20%
 Rule 3 commit leader ratio: 0.00%
 Rule 3 skip leader ratio: 25.21%
 Rule 1 block ratio: 95.30%
 Rule 2 block ratio: 4.70%
 Rule 3 block ratio: 0.00%
 ABA average duration: 283 ms
 ABA maximum duration: 589 ms
 ABA minimum duration: 200 ms

 End-to-end TPS: 48,283 tx/s
 End-to-end BPS: 24,721,029 B/s
 End-to-end latency: 502 ms
-----------------------------------------
~~~

### Preserved adversarial result (`faults = 1`)

For comparison, this is the previously recorded 4-node, 1-fault, 20-second local result using the adversary commands above:

~~~text
-----------------------------------------
 SUMMARY:
-----------------------------------------
 + CONFIG:
 Faults: 1 node(s)
 Committee size: 4 node(s)
 Worker(s) per node: 1 worker(s)
 Collocate primary and workers: True
 Input rate: 50,000 tx/s
 Transaction size: 512 B
 Execution time: 20 s

 Header size: 1,000 B
 Max header delay: 200 ms
 GC depth: 50 round(s)
 Sync retry delay: 10,000 ms
 Sync retry nodes: 3 node(s)
 batch size: 500,000 B
 Max batch delay: 200 ms

 + RESULTS:
 Consensus TPS: 37,518 tx/s
 Consensus BPS: 19,209,358 B/s
 Consensus latency: 443 ms
 Leader commit latency: 276 ms
 Non-leader commit latency: 517 ms
 All committed headers latency: 451 ms
 Leader commit interval: 236 ms
 Non-leader rule-order latency: 512 ms
 Rule 1 leader ratio: 46.00%
 Rule 2 leader ratio: 37.00%
 Rule 3 commit leader ratio: 0.00%
 Rule 3 skip leader ratio: 17.00%
 Rule 1 block ratio: 52.98%
 Rule 2 block ratio: 47.02%
 Rule 3 block ratio: 0.00%
 ABA average duration: 394 ms
 ABA maximum duration: 609 ms
 ABA minimum duration: 207 ms

 End-to-end TPS: 37,316 tx/s
 End-to-end BPS: 19,105,858 B/s
 End-to-end latency: 643 ms
-----------------------------------------
~~~

Results vary with hardware and load. `Consensus latency` measures header creation to consensus commit; `End-to-end latency` starts when the client submits a sampled transaction. ABA duration statistics include completed local ABA node-instances only.

## Next Steps

- Read [Narwhal and Tusk: A DAG-based Mempool and Efficient BFT Consensus](https://arxiv.org/pdf/2105.11827.pdf).
- See [benchmark/README.md](benchmark/README.md) for complete benchmark parameters and result semantics.
- See [README-AWS-10-20-50节点部署.md](README-AWS-10-20-50节点部署.md) for AWS 10/20/50-node deployment, TCP 3005, cross-Region networking, and adversary examples.
- See [README-WINDOWS五区域PEM部署.md](README-WINDOWS五区域PEM部署.md) when a Windows computer controls five AWS Regions using one PEM per Region.
- Inspect the [primary](primary), [worker](worker), and [consensus](consensus) crates, including the ABA implementation in `consensus/src/aba.rs`.

## License

This software is licensed under [Apache License 2.0](LICENSE).
