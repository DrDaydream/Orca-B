# A1 common-clock acceptance

`scripts/a1_acceptance.py` runs the 30 Orca-A/Orca-B A1 configurations on an
existing, configured EC2 testbed. It invokes only `fab remote`; installation and
testbed lifecycle remain separate. Run this campaign after the current Mysticeti
matrix finishes. The script takes the same nonblocking controller lock,
`/tmp/five-protocol-figure-matrix.lock`, and refuses overlapping executions.
Every process using this testbed must cooperate with that lock; it cannot detect
an unrelated manual `fab remote` launched without it.

The script is identical in both repositories. Planning and archive reporting
need Python 3.9+ and the standard library. Execution requires the Linux
controller's configured Fabric environment, `settings.json`, matching installed
`node` and `benchmark_client` binaries, and enough running instances in the
configured regions. The native runner verifies the build marker on the selected
hosts. No AWS work has been performed merely by adding or testing this tool.

## Fixed matrix and explicit defaults

The nominal rates below 100,000 TPS are 10,000, 30,000, 50,000, 70,000 and 90,000.
The rounding is copied from `figure-tools/run_matrix.py` v3.5:

```python
quantum = 20 * nodes
input_rate = ((nominal_rate + quantum // 2) // quantum) * quantum
```

The 10/20/35-node committees use `f = floor((nodes - 1) / 3)` crashes. The last
`f` launch IDs have no primary, worker or client process. Each surviving client
keeps its original rate share; the aggregate offered rate therefore decreases.
Remote proposals retain the native ECHO → READY → FINISH flow. The author
delivers its own fully validated, stored proposal locally immediately, while
continuing normal ECHO/READY participation. This is local delivery, not final
transaction commit; see `../A1-RUNTIME-CLOCK.md`.

| Nodes | Crashed IDs | Active replicas | Input rates (TPS) | Offered rates (TPS) |
|---:|---|---:|---|---|
| 10 | 7–9 | 7 | 10000, 30000, 50000, 70000, 90000 | 7000, 21000, 35000, 49000, 63000 |
| 20 | 14–19 | 14 | 10000, 30000, 50000, 70000, 90000 | 7000, 21000, 35000, 49000, 63000 |
| 35 | 24–34 | 24 | 9800, 30100, 49700, 70000, 90300 | 6720, 20640, 34080, 48000, 61920 |

Defaults are **10 seconds warmup, 60 seconds measurement, 10 seconds drain,
20-byte header, seed 7, 512-byte transactions, one worker and one run per case**.
The other node parameters remain explicit in the validated preset: header delay
200 ms, GC depth 50, sync retry 10,000 ms with 3 nodes, batch size 500,000 bytes,
and maximum batch delay 200 ms. A1 is a full-run process crash with legacy
`faults=0`. Orca-B retains its A5-only ABA scheduling.

Use `--warmup 0 --drain 0` to reproduce the earlier window-length preset, in a
separate campaign directory. This remains a common-clock campaign and is not
interchangeable with old first-client-start archives. Changes to duration,
warmup, drain, header size or the `--variant` label require a new output path.

## Plan and run after the existing matrix

This command is read-only and works on Windows as well as Linux. It does not
require settings, installed binaries or cloud credentials:

```bash
python3 /home/ubuntu/experiments/Orca-A/benchmark/scripts/a1_acceptance.py \
  --root /home/ubuntu/experiments \
  --output /home/ubuntu/experiments/a1-acceptance-clock-v1 \
  --dry-run
```

Use a new, dedicated acceptance output directory. Do not reuse a figure-matrix
output, copy old selected results into it, or update the active figure toolkit.
The full manifest always contains both protocols and all 30 cases. `--protocol`
and `--nodes` only filter the work performed in the current invocation.

After Mysticeti completes, first install Orca-A on the existing testbed using
the normal native installation procedure. For example, on an idle controller
with the intended `settings.json` and committed code already checked out:

```bash
cd /home/ubuntu/experiments/Orca-A/benchmark
flock -n /tmp/five-protocol-figure-matrix.lock fab install

python3 scripts/a1_acceptance.py \
  --root /home/ubuntu/experiments \
  --output /home/ubuntu/experiments/a1-acceptance-clock-v1 \
  --protocol Orca-A
```

The existing testbed must support 35 configured authorities to run all node
counts in one invocation. With smaller installed groups, execute the needed
subset using `--nodes 10`, `--nodes 20` or `--nodes 35` only after the appropriate
group is ready. The acceptance runner never creates or allocates that group.
Keep the recorded settings/source/build identity unchanged within each protocol
campaign; a changed configuration requires a new campaign.

Then install Orca-B and run its group serially against the same output:

```bash
cd /home/ubuntu/experiments/Orca-B/benchmark
flock -n /tmp/five-protocol-figure-matrix.lock fab install

python3 scripts/a1_acceptance.py \
  --root /home/ubuntu/experiments \
  --output /home/ubuntu/experiments/a1-acceptance-clock-v1 \
  --protocol Orca-B
```

Do not run `--protocol all` across shared hosts until the installed binary
selection is correct for each group. Installing Orca-B changes the shared
`./node` and `./benchmark_client` links; the native runner rejects the wrong
repository/build. It is usually clearer to run the two commands above.

Normal resume uses the same command. Every completed selection is hashed and
reparsed from its archived raw logs before it is reused. A valid result above
the latency limit remains a visible failure. Use `--rerun-latency-failures` only
when an explicit repeat is intended; it retains the old attempt and replaces
the selected attempt. This is a repeat, not an independent statistical trial,
and repeated selection should be disclosed when reporting experiment results.
The report shows the number of attempts; metadata records the previous selected
attempt and why a replacement was launched.

An interrupted or failed native run stops execution. Its available evidence is
kept. Confirm the testbed is idle, inspect the failure, then use `--retry-failed`
to make a new attempt. A damaged previously completed archive is rejected;
`--retry-failed` does not silently adopt or repair it. Never remove the global
lock file to bypass a running process: the lock is attached to its open inode.

## Acceptance and limits of the claim

The primary E2E endpoint is **client sample release to final ordered commit**.
The population comprises samples whose final commit occurs in the fixed common
window `[start_us, end_us)`. It can include samples released during warmup.
Each active replica has its own sample mean; the case value is the unweighted
mean of **all** active replica means. The gate is strictly `<3000 ms`, with no
rounding before comparison. It is not a per-replica maximum or p99 guarantee;
the report separately shows the maximum replica mean.

A case needs complete matching client/worker/primary logs for every active
replica, a fresh run ID, matching armed plans and parameter snapshots, valid
instrumentation and timestamp ordering, and positive completed sample counts
and valid throughput on every active replica. The saved native parser recomputes
the metrics from raw evidence and must agree with the native summary. Empty or
excluded replicas, stale files, wrong configurations, missing measurements,
non-finite values and altered archives cannot produce a pass. SHA256 manifests
are integrity checks, not signatures against an adversary who rewrites the
whole campaign and its manifests.

The release-cohort diagnostic follows samples actually released in
`[start_us, end_us)` through the exclusive cutoff `drain_end_us`. Reports include
its mean per-replica E2E, completed and unfinished sample-replica observations,
not-received observations, minimum and mean completion ratio, and fixed-window
final TPS. `WINDOW DISAGREEMENT` flags differing `<3000 ms` decisions between
the two populations. Sample counts summed across replicas are **not unique
transaction counts**. The TPS denominator is the configured measured duration;
bytes come from final committed batch payloads and the 512-byte transaction
size, independently of how often transactions are latency-sampled.

There is deliberately **no minimum completion-ratio or TPS threshold**. A pass
proves the stated latency criterion for valid observed completions, conditional
on the displayed completion coverage. It does not prove that all offered load
was sustained or that unfinished transactions would finish quickly. Low
coverage or low TPS must remain visible when results are interpreted. There is
one run per case, so this campaign does not supply confidence intervals or a
multi-run tail-latency claim. Native UTC clocks must be synchronized; the
benchmark's common start/stop handshake does not itself eliminate machine
clock error.

## Artifacts and offline reporting

Each attempt has raw logs, console output, the native comparison JSON and native
text/JSONL summary for that attempt, its parameter snapshot, public committee
and node configuration, `measurement-window.json`, the exact native measurement
parser and acceptance runner, settings, provenance, acceptance metrics and a
SHA256 manifest. Each attempt has a fresh log directory. A failed attempt
retains the logs collected from that run and any collection-issues.json produced
during teardown. The archive checks one configured rate, transaction size and
worker target declaration per active client against the planned workload and
the saved committee in launch order. Private node keys and SSH private-key
contents are not copied.
Settings can contain infrastructure addresses and a private-key **path**, so
treat the artifacts as operational experiment data.

The provenance pins the controller commit, installed controller binary hashes
and marker, benchmark Python source hashes, settings hash, explicit case
configuration, variant label and relevant benchmark environment. Remote binary
verification uses the native runner's installed-commit marker checks. The
acceptance runner does not independently hash remote executables.

`acceptance-report.json` includes full per-replica decisions and all 30 planned
cases, including missing/invalid ones. `acceptance-report.md` presents the main
metrics as a table. Rebuild the reports without Fabric or AWS access:

```bash
python3 /home/ubuntu/experiments/Orca-A/benchmark/scripts/a1_acceptance.py \
  --output /home/ubuntu/experiments/a1-acceptance-clock-v1 \
  --report-only
```

Exit status `0` means **all 30 cases are present, valid and below the limit**.
Exit status `1` means the full campaign is incomplete, invalid during offline
reporting, or contains a valid latency failure. The first protocol group normally
returns `1` because the other group is still missing. Exit status `2` identifies
execution, configuration or validation errors; status `130` is interruption.
Inspect the saved report after each group. Do not put partial group commands
under an unqualified `set -e` workflow that would mistake expected status `1`
for permission to discard the remaining group.

The offline regression suite includes a synthetic full 30-case archive using
the actual native parser. This checks the runner and archive contract, **not
EC2 performance**:

```bash
cd /home/ubuntu/experiments/Orca-A/benchmark
python3 -m unittest discover -s tests -p test_a1_acceptance.py -v
```
