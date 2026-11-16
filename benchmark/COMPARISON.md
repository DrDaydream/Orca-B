# Fixed-revision local comparison on Ubuntu 24.04

The requested baseline is **MystenLabs/narwhal, branch Bullshark,
3257759165a2f9ec09157509294146adcc0c2393**. At this revision
consensus/src/lib.rs exports tusk::Consensus. Keep this provenance in results;
the branch name does not establish results for other Bullshark implementations.
The patch changes only Python launching/statistics and locked builds.
It preserves the baseline Rust code and Cargo.lock.

Orca validates and serializes a local complete proposal once, then overlaps
its existing broadcast and atomic storage operation. Core still receives the
header only after successful storage. GRBC/ABA thresholds and commit rules are
unchanged. This removes a serial wait; its performance benefit is unmeasured.

Paste the commands directly into Bash; no virtual environment or script file is
needed. Use an otherwise idle x86_64 host with SSE4.2/PCLMUL, such as the stated
m5.4xlarge. Both builds use Rust 1.85, Clang 14, hardware CRC flags, LTO and one
release codegen unit. Tokio thread settings remain at default. Build jobs limit
compiler parallelism only. Each application retains its own dependency versions.
Set ORCA_REV to a full candidate commit to reproduce a particular run; otherwise
the current candidate branch is fetched and its resolved SHA is recorded.
The commands also run pre-change Orca 0ee8dc1 with the same harness and lockfile,
so throughput preservation can be evaluated against the previous Orca version.

~~~bash
set -euo pipefail
sudo apt-get update
sudo apt-get install -y build-essential cmake clang-14 libclang-14-dev \
  pkg-config libssl-dev protobuf-compiler libprotobuf-dev curl git python3 python3-pip
if ! command -v rustup >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
fi
source "$HOME/.cargo/env"
rustup toolchain install 1.85.0 --profile minimal
export PATH="$HOME/.local/bin:$PATH"
export RUSTUP_TOOLCHAIN=1.85.0 PYTHONUNBUFFERED=1
unset TOKIO_WORKER_THREADS CARGO_ENCODED_RUSTFLAGS CARGO_TARGET_DIR RUST_LOG
export LIBCLANG_PATH=/usr/lib/llvm-14/lib CLANG_PATH=/usr/bin/clang-14
export CC=/usr/bin/clang-14 CXX=/usr/bin/clang++-14
export CXXFLAGS='-include cstdint'
export RUSTFLAGS='-C target-feature=+sse4.2,+pclmulqdq'
export CARGO_PROFILE_RELEASE_LTO=true CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1
export CARGO_BUILD_JOBS=4 PROTOC=/usr/bin/protoc
export COMPARE_ROOT="$(mktemp -d "$HOME/orca-bullshark-compare-XXXXXX")"
export ORCA_REV="${ORCA_REV:-orca-b-preparsed-vote-keys}"
export BASELINE_REV=3257759165a2f9ec09157509294146adcc0c2393
export ORCA_BEFORE_REV=0ee8dc10312ad492ae99e21b70da9df9a373fdf4
mkdir -p "$COMPARE_ROOT/results" "$COMPARE_ROOT/metadata"
printf 'Comparison directory: %s\n' "$COMPARE_ROOT"

git init "$COMPARE_ROOT/orca"
git -C "$COMPARE_ROOT/orca" remote add origin https://github.com/DrDaydream/Orca-B.git
git -C "$COMPARE_ROOT/orca" fetch --depth 1 origin "$ORCA_REV"
git -C "$COMPARE_ROOT/orca" checkout --detach FETCH_HEAD
git init "$COMPARE_ROOT/orca-before"
git -C "$COMPARE_ROOT/orca-before" remote add origin https://github.com/DrDaydream/Orca-B.git
git -C "$COMPARE_ROOT/orca-before" fetch --depth 1 origin "$ORCA_BEFORE_REV"
git -C "$COMPARE_ROOT/orca-before" checkout --detach FETCH_HEAD
cp -a "$COMPARE_ROOT/orca/benchmark/." "$COMPARE_ROOT/orca-before/benchmark/"
cp "$COMPARE_ROOT/orca/Cargo.lock" "$COMPARE_ROOT/orca-before/Cargo.lock"
git init "$COMPARE_ROOT/bullshark-3257759"
git -C "$COMPARE_ROOT/bullshark-3257759" remote add origin https://github.com/MystenLabs/narwhal.git
git -C "$COMPARE_ROOT/bullshark-3257759" fetch --depth 1 origin "$BASELINE_REV"
git -C "$COMPARE_ROOT/bullshark-3257759" checkout --detach FETCH_HEAD
test "$(git -C "$COMPARE_ROOT/bullshark-3257759" rev-parse HEAD)" = "$BASELINE_REV"
python3 -m pip install --user --break-system-packages \
  -r "$COMPARE_ROOT/orca/benchmark/requirements-comparison.txt"
git -C "$COMPARE_ROOT/bullshark-3257759" apply --check \
  "$COMPARE_ROOT/orca/benchmark/bullshark-local.patch"
git -C "$COMPARE_ROOT/bullshark-3257759" apply \
  "$COMPARE_ROOT/orca/benchmark/bullshark-local.patch"
cp "$COMPARE_ROOT/orca/benchmark/benchmark/local_processes.py" \
  "$COMPARE_ROOT/bullshark-3257759/benchmark/benchmark/local_processes.py"
cp "$COMPARE_ROOT/orca/benchmark/benchmark/logs.py" \
  "$COMPARE_ROOT/bullshark-3257759/benchmark/benchmark/comparison_logs.py"

uname -a > "$COMPARE_ROOT/metadata/uname.txt"
lscpu > "$COMPARE_ROOT/metadata/lscpu.txt"
free -h > "$COMPARE_ROOT/metadata/memory.txt"
rustc -Vv > "$COMPARE_ROOT/metadata/rustc.txt"
clang-14 --version > "$COMPARE_ROOT/metadata/clang.txt"
protoc --version > "$COMPARE_ROOT/metadata/protoc.txt"
python3 -m pip freeze > "$COMPARE_ROOT/metadata/python-packages.txt"
python3 - <<'PY'
import json, os
from pathlib import Path
flags = Path('/proc/cpuinfo').read_text()
assert 'sse4_2' in flags and 'pclmulqdq' in flags, 'These flags require SSE4.2/PCLMUL'
names = ['RUSTUP_TOOLCHAIN', 'RUSTFLAGS', 'CXXFLAGS', 'CC', 'CXX', 'PROTOC',
         'CARGO_PROFILE_RELEASE_LTO', 'CARGO_PROFILE_RELEASE_CODEGEN_UNITS',
         'CARGO_BUILD_JOBS', 'TOKIO_WORKER_THREADS']
(Path(os.environ['COMPARE_ROOT']) / 'metadata/build-env.json').write_text(
    json.dumps({name: os.environ.get(name) for name in names}, indent=2))
PY
for implementation in orca orca-before bullshark-3257759; do
  (
    cd "$COMPARE_ROOT/$implementation"
    git rev-parse HEAD > "$COMPARE_ROOT/metadata/$implementation.commit"
    git diff > "$COMPARE_ROOT/metadata/$implementation.patch"
    cp Cargo.lock "$COMPARE_ROOT/metadata/$implementation.Cargo.lock"
    cargo build --locked --release -p node --features benchmark \
      2>&1 | tee "$COMPARE_ROOT/metadata/$implementation-build.txt"
    sha256sum target/release/node target/release/benchmark_client \
      > "$COMPARE_ROOT/metadata/$implementation-binaries.sha256"
  )
done
cp "$COMPARE_ROOT/orca/benchmark/bullshark-local.patch" "$COMPARE_ROOT/metadata/"
cp "$COMPARE_ROOT/orca/benchmark/benchmark/local_processes.py" "$COMPARE_ROOT/metadata/"
cp "$COMPARE_ROOT/orca/benchmark/benchmark/logs.py" "$COMPARE_ROOT/metadata/comparison_logs.py"
(cd "$COMPARE_ROOT/orca/benchmark" && python3 -m unittest discover -s tests -v)

for repetition in 1 2 3; do
  for nodes in 4 10 20 30; do
    case "$repetition" in
      1) order=(orca orca-before bullshark-3257759) ;;
      2) order=(bullshark-3257759 orca orca-before) ;;
      3) order=(orca-before bullshark-3257759 orca) ;;
    esac
    for implementation in "${order[@]}"; do
      output="$COMPARE_ROOT/results/$implementation-n$nodes-r$repetition"
      mkdir -p "$output"
      (
        cd "$COMPARE_ROOT/$implementation/benchmark"
        run_status=0
        fab local --nodes "$nodes" --header-size 20 --duration 20 \
          > "$output/summary.txt" 2>&1 || run_status=$?
        printf '%s\n' "$run_status" > "$output/exit-status.txt"
        if [ -d logs ]; then cp -a logs "$output/"; fi
        for config in .committee.json .parameters.json; do
          if [ -f "$config" ]; then cp "$config" "$output/"; fi
        done
        for database in .db-*/LOG; do
          if [ -f "$database" ]; then
            cp "$database" "$output/rocksdb-${database%/LOG}.log"
          fi
        done
        cat "$output/summary.txt"
      )
    done
  done
done
python3 - <<'PY' | tee "$COMPARE_ROOT/results/comparison.txt"
import json, os
from pathlib import Path
from statistics import median
root = Path(os.environ['COMPARE_ROOT']) / 'results'
for n in [4, 10, 20, 30]:
    values = {}
    for implementation in ['orca', 'orca-before', 'bullshark-3257759']:
        rows = []
        for r in [1, 2, 3]:
            folder = root / f'{implementation}-n{n}-r{r}'
            if (folder / 'exit-status.txt').read_text().strip() != '0':
                continue
            path = folder / 'logs/comparison.json'
            if path.exists():
                rows.append(json.loads(path.read_text()))
        if len(rows) != 3 or any(x['final_e2e_mean_ms'] is None for x in rows):
            print(n, implementation, 'INVALID: inspect failed/unfinished runs')
            continue
        keys = ['final_tps', 'final_consensus_mean_ms', 'final_e2e_mean_ms',
                'final_e2e_p95_ms', 'sample_completion_ratio']
        values[implementation] = {k: median(x[k] for x in rows) for k in keys}
        print(n, implementation, json.dumps(values[implementation], sort_keys=True))
    if 'orca' in values and 'bullshark-3257759' in values:
        a, b = values['orca'], values['bullshark-3257759']
        if b['final_e2e_mean_ms'] > 0 and b['final_tps'] > 0:
            print(n, 'E2E reduction %:', round(100 * (1 - a['final_e2e_mean_ms'] /
                                                     b['final_e2e_mean_ms']), 2),
                  'TPS ratio:', round(a['final_tps'] / b['final_tps'], 4))
    if 'orca' in values and 'orca-before' in values:
        a, b = values['orca'], values['orca-before']
        if b['final_e2e_mean_ms'] > 0 and b['final_tps'] > 0:
            print(n, 'Versus previous Orca: E2E reduction %:',
                  round(100 * (1 - a['final_e2e_mean_ms'] / b['final_e2e_mean_ms']), 2),
                  'TPS ratio:', round(a['final_tps'] / b['final_tps'], 4))
PY
tar -czf "$COMPARE_ROOT.tar.gz" -C "$COMPARE_ROOT" results metadata
printf 'Results: %s\nArchive: %s.tar.gz\n' "$COMPARE_ROOT/results" "$COMPARE_ROOT"
~~~

For a separate Orca correctness check in the same environment:

~~~bash
cd "$COMPARE_ROOT/orca"
cargo test --workspace --all-features --locked -- --test-threads=1
~~~

Use FINAL ORDERED COMPARISON and logs/comparison.json, not the preceding legacy
SUMMARY. The window is [earliest client Start, Start + 20 seconds).
The pinned baseline logs worker digests as `Ok(digest)` and primary digests as
`digest`; the shared parser normalizes the worker wrapper for both byte counts
and sampled transactions. Older comparison runs with the error `Final commit is
missing its batch size or proposal` can be reparsed from their saved logs after
updating this Python parser. Preserve the original exit status and record the
parser revision separately; a parsing repair is not a new benchmark run.
TPS always divides by 20 seconds. Final consensus latency starts at proposal
creation; final E2E starts at sampled client submission. Both stop at the first
observed replica's final ordered commit, not application execution or completion
at every replica. Mean/p95 describe completed samples; unfinished samples remain
explicitly reported.

Evaluate 40% separately at each committee size: Orca E2E / baseline E2E <= 0.60.
Check fixed-window TPS, sample completion and p95 together; lower latency with
fewer completed transactions does not establish the goal. Repeated 20-second
runs test this workload; they do not establish sustained capacity or an
asynchronous worst-case latency bound. No 40% improvement is yet demonstrated.
The orca-before results use pre-change Orca 0ee8dc1 with the same Python harness,
dependency lock and build environment to check for throughput regressions.

The local Windows Orca tests/release build pass. The exact baseline depends on
pprof APIs unavailable on Windows, so its full build and the matched experiments
still require Ubuntu. Do not change its Rust protocol source to bypass that
platform restriction.
