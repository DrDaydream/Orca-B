# Orca-B 50 节点并行下载、依赖安装与编译

在 node0 上执行。节点用户为 `ubuntu`，SSH 密钥由 `/home/ubuntu/.ssh/config` 按 Region 自动选择；`deploy/hosts-50.txt` 每行填写一个私网 IPv4，第一行是 node0。

~~~bash
cd /home/ubuntu/Orca-B
HOSTS=deploy/hosts-50.txt
sed -e 's/#.*//' -e '/^[[:space:]]*$/d' "$HOSTS" | wc -l
~~~

并行下载或更新代码：

~~~bash
sed -e 's/#.*//' -e '/^[[:space:]]*$/d' "$HOSTS" | xargs -P 50 -I {} ssh {} '
  if [ -d /home/ubuntu/Orca-B/.git ]; then git -C /home/ubuntu/Orca-B pull --ff-only;
  elif [ ! -e /home/ubuntu/Orca-B ]; then git clone https://github.com/DrDaydream/Orca-B.git /home/ubuntu/Orca-B;
  else echo "existing non-git directory" >&2; exit 1; fi'
~~~

并行安装依赖、下载 Cargo 依赖并编译：

~~~bash
sed -e 's/#.*//' -e '/^[[:space:]]*$/d' "$HOSTS" | xargs -P 10 -I {} ssh {} '
  set -e
  sudo apt-get update
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y build-essential clang libclang-dev cmake pkg-config libssl-dev librocksdb-dev git curl
  if ! command -v cargo >/dev/null 2>&1; then curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y; fi
  cd /home/ubuntu/Orca-B
  . "$HOME/.cargo/env" 2>/dev/null || true
  cargo fetch
  CARGO_BUILD_JOBS=2 cargo build --quiet --release --features benchmark
'
~~~

检查所有节点：

~~~bash
sed -e 's/#.*//' -e '/^[[:space:]]*$/d' "$HOSTS" | xargs -P 50 -I {} ssh {} '
  printf "%s: " "$(hostname)"; test -x /home/ubuntu/Orca-B/target/release/node && echo "build ok" || echo "build failed"'
~~~

如跨洲网络拥塞，将 `xargs -P 50` 降为 `-P 10` 或 `-P 20`。
