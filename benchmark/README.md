# Running Benchmarks
This document explains how to benchmark the codebase and read benchmarks' results. It also provides a step-by-step tutorial to run benchmarks on [Amazon Web Services (AWS)](https://aws.amazon.com) accross multiple data centers (WAN).

## Local Benchmarks
For the fixed MystenLabs/narwhal comparison, use [COMPARISON.md](COMPARISON.md).
Both runners accept `fab local --nodes 20 --header-size 20 --duration 20`
and report final ordered commits in the same fixed measurement window.

When running benchmarks, the codebase is automatically compiled with the feature flag `benchmark`. This enables the node to print some special log entries that are then read by the python scripts and used to compute performance. These special log entries are clearly indicated with comments in the code: make sure to not alter them (otherwise the benchmark scripts will fail to interpret the logs).

### Parametrize the benchmark
After cloning the repo and [installing all dependencies](https://github.com/asonnino/narwhal#quick-start), you can use [Fabric](http://www.fabfile.org/) to run benchmarks on your local machine.  Locate the task called `local` in the file [fabfile.py](https://github.com/asonnino/narwhal/blob/master/benchmark/fabfile.py):
```python
@task
def local(ctx):
    ...
```
The task specifies two types of parameters, the *benchmark parameters* and the *nodes parameters*. The benchmark parameters look as follows:
```python
bench_params = {
    'nodes': 4,
    'workers': 1,
    'rate': 50_000,
    'tx_size': 512,
    'faults': 0,
    'duration': 20,
}
```
They specify the number of primaries (`nodes`) and workers per primary (`workers`) to deploy, the input rate (tx/s) at which the clients submits transactions to the system (`rate`), the size of each transaction in bytes (`tx_size`), the adversarial-scheduling parameter (`faults`), and the duration of the benchmark in seconds (`duration`). The minimum transaction size is 9 bytes, this ensure that the transactions of a client are all different. The benchmarking script will deploy as many clients as workers and divide the input rate equally amongst each client. For instance, if you configure the testbed with 4 nodes, 1 worker per node, and an input rate of 1,000 tx/s (as in the example above), the scripts will deploy 4 clients each submitting transactions to one node at a rate of 250 tx/s.

Orca-B note: when `faults > 0`, all nodes remain online. In every round, all primaries use `ORCA_ADVERSARY_SEED`, the round, and the ordered committee to select the same pseudo-random set of `faults` adversarial authorities. A selected adversarial leader is forced into Rule 3 and cannot complete through Rule 1 or Rule 2; a non-adversarial leader checks Rule 1 and Rule 2 normally before falling back to Rule 3. Non-leader adversaries are silent. `ORCA_RULE3_BEHAVIOR=mixed` (the default) deterministically chooses whether an adversarial Rule-3 leader is silent or participates; `silent` and `participate` force either behavior. Silent authorities suppress their header and pause local batch production while continuing to receive messages; ABA remains independent. The default seed is `0`; set, for example, `ORCA_ADVERSARY_SEED=42 fab local` to obtain another reproducible schedule. Here `faults` controls the adversary set rather than omitting the last nodes.

When `faults > 0`, a separate deterministic coin defers approximately half of the non-adversarial leaders past Rule 1 so that they wait for Rule 2. The remaining leaders attempt Rule 1 normally, and a failed Rule-1 attempt may still use Rule 2. Orca-B's existing early-ABA accounting remains part of Rule 2. Consequently the measured Rule-1-to-Rule-2 ratio approaches 1:1 over long runs without preventing Rule 2 from preserving liveness. The printed percentages use all leaders, including Rule 3, as their denominator, so Rule 1 and Rule 2 are not expected to each print exactly 50%.

Client traffic during adversarial silence is selected with `ORCA_CLIENT_DURING_SILENCE`. Its default, `send`, keeps clients sending while the local BatchMaker is paused. `pause` gives each client a pre-generated deterministic silence bitmap, so it sends no transactions during its silent time slots. This is one-way and uses no Worker-to-Client control traffic. A slot defaults to `max_header_delay` milliseconds and can be overridden with `ORCA_CLIENT_SILENCE_SLOT_MS`. For example: `ORCA_CLIENT_DURING_SILENCE=pause ORCA_CLIENT_SILENCE_SLOT_MS=200 fab local`. The bitmap is a wall-clock workload schedule; it does not query the protocol's live round.

ABA uses the dedicated `aba_to_aba` committee address. On AWS deployments this is TCP port 3005; GRBC, READY, leader recovery, and synchronization remain on the primary-to-primary port 3000. ABA has its own reliable sender, TCP connections, ingress receiver, signature-verification path, and internal command queue before messages enter the ordered consensus state machine. All nodes must use the updated committee schema and binary together.

The nodes parameters determine the configuration for the primaries and workers:
```python
node_params = {
    'header_size': 1_000,
    'max_header_delay': 100,
    'gc_depth': 50,
    'sync_retry_delay': 10_000,
    'sync_retry_nodes': 3,
    'batch_size': 500_000,
    'max_batch_delay': 100
}
```
They are defined as follows:
* `header_size`: The preferred header size. The primary creates a new header when it has enough parents and enough batches' digests to reach `header_size`. Denominated in bytes.
* `max_header_delay`: The maximum delay that the primary waits between generating two headers, even if the header did not reach `max_header_size`. Denominated in ms.
* `gc_depth`: The depth of the garbage collection (Denominated in number of rounds).
* `sync_retry_delay`: The delay after which the synchronizer retries to send sync requests. Denominated in ms.
* `sync_retry_nodes`: Determine with how many nodes to sync when re-trying to send sync-request. These nodes are picked at random from the committee.
* `batch_size`: The preferred batch size. The workers seal a batch of transactions when it reaches this size. Denominated in bytes.
* `max_batch_delay`: The delay after which the workers seal a batch of transactions, even if `max_batch_size` is not reached. Denominated in ms.

### Run the benchmark
Once you specified both `bench_params` and `node_params` as desired, run:
```
$ fab local
```
On x86_64 Linux, the repository's Cargo configuration enables SSE4.2 and PCLMUL
for RocksDB hardware CRC32C. Every server running this binary must support both
CPU features. The flags do not apply to ARM or Windows targets. Existing
`RUSTFLAGS` or `CARGO_ENCODED_RUSTFLAGS` environment variables override Cargo's
configured flags; unset them to use the repository defaults. A change to these
flags triggers recompilation, so the first run can spend longer in
`Setting up testbed...`.

After the run, check the primary and worker database logs from this directory:
```bash
grep -H 'Fast CRC32 supported' .db-*/LOG
```
Each database should report `Supported on x86`.

This command first recompiles your code in `release` mode (and with the `benchmark` feature flag activated), thus ensuring you always benchmark the latest version of your code. This may take a long time the first time you run it. It then generates the configuration files and keys for each node, and runs the benchmarks with the specified parameters. It finally parses the logs and displays a summary of the execution similarly to the one below. All the configuration and key files are hidden JSON files; i.e., their name starts with a dot (`.`), such as `.committee.json`.
```
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
 Execution time: 19 s

 Header size: 1,000 B
 Max header delay: 100 ms
 GC depth: 50 round(s)
 Sync retry delay: 10,000 ms
 Sync retry nodes: 3 node(s)
 batch size: 500,000 B
 Max batch delay: 100 ms

 + RESULTS:
 Consensus TPS: 46,478 tx/s
 Consensus BPS: 23,796,531 B/s
 Consensus latency: 464 ms

 End-to-end TPS: 46,149 tx/s
 End-to-end BPS: 23,628,541 B/s
 End-to-end latency: 557 ms
-----------------------------------------
```
The 'Consensus TPS' and 'Consensus latency' respectively report the average throughput and latency without considering the client. The consensus latency thus refers to the time elapsed between the block's creation and its commit. In contrast, 'End-to-end TPS' and 'End-to-end latency' report the performance of the whole system, starting from when the client submits the transaction. The end-to-end latency is often called 'client-perceived latency'. To accurately measure this value without degrading performance, the client periodically submits 'sample' transactions that are tracked across all the modules until they get committed into a block; the benchmark scripts use sample transactions to estimate the end-to-end latency.

## AWS Benchmarks

### Ubuntu 24.04 controller and testbed

Run Fabric from the outer `benchmark` directory containing `fabfile.py`.
The controller needs the Python packages in `requirements.txt`, an RSA SSH
private key, and AWS API credentials. An attached EC2 IAM role works through
Boto3's normal credential lookup; a named AWS profile is only needed if it
actually exists on the controller. An SSH key pair name is independent of an
AWS credential profile name.

The checked-in `settings.example.json` configures `orca-amazon`, its private key
path, the Orca-B development branch, and `m5.8xlarge` in five regions. Copy it
once to create the local configuration:

```bash
cd /home/ubuntu/experiments/Orca-B-grbc/benchmark
cp -n settings.example.json settings.json
```

Edit the private key path and other settings for your environment. The local
`settings.json` is ignored by Git. Import the same RSA public key under the name
`orca-amazon` in every configured region; keep the private key on the controller.
Use Ubuntu 24.04 x86_64 instances with public IPv4 connectivity. The instance
manager discovers the latest available Canonical Ubuntu 24.04 image in each region.

For this EC2 controller, using the attached IAM role:

```bash
cd /home/ubuntu/experiments/Orca-B-grbc/benchmark
unset AWS_PROFILE AWS_DEFAULT_PROFILE
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
fab info
```

### Create the testbed

If the testbed has not been created, the following creates four instances in
**each** of the five regions, for 20 instances total:

```bash
cd /home/ubuntu/experiments/Orca-B-grbc/benchmark
fab create --nodes 4
fab info
```

Each instance has a 200 GiB gp2 root volume. Creation adds instances on each
invocation; inspect partial results before retrying a failed creation. The
manager identifies instances by `Name=dag-node`, so keep the controller and
unrelated instances outside that tag. The default `dag` security group opens SSH
and the benchmark's TCP port range; restrict access to your deployment's sources.

### Run directly with Fabric

Once the instances exist, run:

```bash
cd /home/ubuntu/experiments/Orca-B-grbc/benchmark
fab remote
```

`fab remote` checks the controller and selected instances for the native build
dependencies, including Clang 14, libclang 14, tmux, and chrony. It installs missing
packages and Rust 1.85.0, waits for clock synchronization, and clones the configured
repository on new instances. Existing packages and the installed Rust toolchain
are reused. `fab install` remains available to prepare the environment separately.
Both commands require sudo access for system packages and services.

All selected instances then fetch the configured branch and check out the
controller's current **HEAD commit**, rather than independently choosing the
latest branch tip. Update the controller first when testing a newer commit. That
commit must be available in the configured remote repository; uncommitted Rust
changes on the controller are not deployed. A reused testbed checkout with
uncommitted tracked changes is rejected before compilation; save or revert those
changes before retrying. The runner uses Rust 1.85.0 and the
repository's Cargo configuration, clearing environment overrides for Rust flags
and Tokio worker threads. This enables the configured hardware CRC32C build on
compatible x86_64 CPUs.

The default remote experiment uses 20 nodes, one collocated worker per node,
`faults=0`, a total input rate of 50,000 tx/s, 512-byte transactions, an 80-second
runtime parameter, one run, and a 20-byte header size. Set the desired parameters
in `remote()` in `fabfile.py`. Keep one node count, one input rate, and `runs=1`
per invocation to avoid reusing committee or database state between configurations.
The runtime parameter excludes dependency preparation, compilation, and log transfer.

Results are printed in the terminal and appended under `results/`; downloaded
raw logs are placed under `logs/`. Save raw logs and the controller commit before
another run because the runner replaces `logs/`. A failed installation, build,
or benchmark is reported as an error to the calling shell.

`fab kill` stops benchmark processes; it does not stop EC2 instances. `fab stop`
stops the testbed, `fab start --max 4` starts up to four instances per region, and
`fab destroy` terminates the tagged instances. These commands operate on the
`Name=dag-node` instances in all configured regions. Stopped instances retain
their EBS volumes.

### Step 6. Plot the results
Once you have enough results, you can aggregate and plot them:
```
$ fab plot
```
This command creates a latency graph, a throughput graph, and a robustness graph in a folder called `plots` (which is automatically created if it doesn't already exists). You can adjust the plot parameters to filter which curves to add to the plot:
```python
plot_params = {
    'faults': [0],
    'nodes': [10, 20, 50],
    'workers': [1],
    'collocate': True,
    'tx_size': 512,
    'max_latency': [3_500, 4_500]
}
```

The first graph ('latency') plots the latency versus the throughput. It shows that the latency is low until a fairly neat threshold after which it drastically increases. Determining this threshold is crucial to understand the limits of the system. 

Another challenge is comparing apples-to-apples between different deployments of the system. The challenge here is again that latency and throughput are interdependent, as a result a throughput/number of nodes chart could be tricky to produce fairly. The way to do it is to define a maximum latency and measure the throughput at this point instead of simply pushing every system to its peak throughput (where latency is meaningless). The second graph ('tps') plots the maximum achievable throughput under a maximum latency for different numbers of nodes.
