# Orca-B AWS EC2 10/20/50 节点部署

每台 EC2 运行一个 Primary、一个 Worker 和一个 benchmark client。Node 0 同时作为控制机。所有协议通信必须使用私网 IP。

> 最新敌手模式：benchmark 中 `faults > 0` 时，所有节点仍保持在线。每轮根据 `ORCA_ADVERSARY_SEED`、轮次和委员会确定性伪随机选出 `f` 个敌手。若本轮 leader 被选为敌手，它会强制进入 Rule 3，不再检查 Rule 1/2；非敌手 leader 仍正常检查 Rule 1/2，未命中才进入 Rule 3。非 leader 敌手静默。`ORCA_RULE3_BEHAVIOR=mixed`（默认）根据相同种子确定该 Rule 3 leader 静默或参与，也可设为 `silent` 或 `participate`。静默节点暂停 Header 和本地 batch 生产，但继续接收协议消息；ABA 独立运行。

当 `faults > 0` 时，非敌手 leader 还会通过独立的确定性随机硬币分流：约一半正常尝试 Rule 1，另一半跳过 Rule 1 等待 Rule 2，使长时间测试中的 Rule 1:Rule 2 接近 1:1。Orca-B 原有的较早 ABA 结果仍计入 Rule 2。统计百分比以包含 Rule 3 的全部 leader 为分母，因此两项不一定各为 50%。

Client 在敌手静默时间段的行为由 `ORCA_CLIENT_DURING_SILENCE` 控制：`send`（默认）继续发送，`pause` 按 benchmark 启动前生成的单向墙钟时间表暂停发送，不使用 Worker → Client 控制消息。时间槽默认等于 `max_header_delay`，也可设置 `ORCA_CLIENT_SILENCE_SLOT_MS`。

```bash
# 本地测试先把 benchmark/fabfile.py 中的 faults 改为大于 0。
ORCA_CLIENT_DURING_SILENCE=send fab local
ORCA_CLIENT_DURING_SILENCE=pause ORCA_CLIENT_SILENCE_SLOT_MS=200 fab local
ORCA_FAULTS=1 ORCA_CLIENT_DURING_SILENCE=pause ./run-multi-servers.sh 10 20 10000
```

## 1. AWS 资源

- Ubuntu Server 24.04 LTS x86_64。
- 所有实例位于同一 Region、VPC，建议同一 Availability Zone。
- 10 节点：建议每台至少 4 vCPU / 16 GiB RAM。
- 20/50 节点：READY 验签工作队列无上限，建议 8 vCPU / 32 GiB RAM 并监控 RSS。
- 磁盘至少 30 GiB gp3。

安全组入站规则：

| 端口 | 来源 | 用途 |
|---:|---|---|
| 22/TCP | 你的 IP 和安全组自身 | SSH |
| 3000–3005/TCP | 安全组自身 | Orca-B 私网通信 |

不要将 3000–3005 开放给 `0.0.0.0/0`。

| 端口 | 用途 |
|---:|---|
| 3000 | Primary ↔ Primary（GRBC、READY、同步） |
| 3001 | Worker → Primary |
| 3002 | Primary → Worker |
| 3003 | Client → Worker |
| 3004 | Worker ↔ Worker |
| 3005 | ABA ↔ ABA（独立发送、接收和验签队列） |

## 2. Node 0 连接所有节点

将 AWS pem 放到 Node 0 的 `~/.ssh/orca-cluster-key.pem`，权限设为 400。创建 `~/.ssh/config`：

```text
Host 10.*
    User ubuntu
    IdentityFile /home/ubuntu/.ssh/orca-cluster-key.pem
    StrictHostKeyChecking accept-new
    ConnectTimeout 8
```

在项目中创建 `deploy/hosts-10.txt`、`hosts-20.txt` 或 `hosts-50.txt`，每行一个私网 IP，第一行是 Node 0。

## 3. 在每台机器安装和编译

```bash
sudo apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
  build-essential cmake clang-14 libclang-14-dev git curl tmux jq \
  python3 python3-pip netcat-openbsd chrony
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"
git clone https://github.com/DrDaydream/Orca-B.git "$HOME/Orca-B"
cd "$HOME/Orca-B"
git pull --ff-only
LIBCLANG_PATH=/usr/lib/llvm-14/lib CLANG_PATH=/usr/bin/clang-14 \
CC=/usr/bin/clang-14 CXX=/usr/bin/clang++-14 CXXFLAGS='-include cstdint' \
cargo build --release --features benchmark
python3 -m pip install --user --break-system-packages -r benchmark/requirements.txt
```

**所有机器必须运行完全相同的 Git commit。** 新旧二进制混用可能导致网络消息反序列化失败。检查：

```bash
while read -r ip; do ssh "$ip" 'git -C ~/Orca-B rev-parse HEAD'; done < deploy/hosts-10.txt
```

## 4. 生成密钥和委员会

在 Node 0 生成 N 个密钥：

```bash
cd ~/Orca-B
mkdir -p deploy
for i in $(seq 0 9); do ./target/release/node generate_keys --filename "deploy/node-${i}.json"; done
```

20/50 节点将 `9` 改为 `19`/`49`。也可以直接使用仓库脚本生成并分发全部配置：

```bash
cd ~/Orca-B
chmod +x prepare-aws-cluster.sh
./prepare-aws-cluster.sh 10
```

脚本根据 hosts 文件生成 `deploy/committee.json`：每个 authority 的常规端口是对应私网 IP 的 3000–3004，ABA 独立使用 3005，stake 为 1，worker id 为 0，并校验所有服务器上的 committee SHA-256。

`deploy/parameters.json` 建议：

```json
{
  "batch_size": 500000,
  "gc_depth": 50,
  "header_size": 1000,
  "max_batch_delay": 200,
  "max_header_delay": 2000,
  "sync_retry_delay": 10000,
  "sync_retry_nodes": 3
}
```

若主要比较延迟，三个协议必须使用同一 `max_header_delay`。不要用 Orca-B 200 ms 直接对比 Bullshark 2000 ms。

将 `node-i.json`、`committee.json` 和 `parameters.json` 分发到对应机器的 `~/Orca-B/deploy/`。分发后比较所有 `committee.json` 的 SHA-256。

## 5. 运行

Node 0：

```bash
cd ~/Orca-B
chmod +x run-multi-servers.sh
./run-multi-servers.sh 10 20 10000
./run-multi-servers.sh 20 60 20000
./run-multi-servers.sh 50 60 50000
```

参数中的 TPS 是**集群总输入速率**，脚本会分摊给每个 client。脚本会先等待所有 client 连通全部 Worker，再计时，最后下载日志并调用 `LogParser`。

最新结果在原有 TPS、`Consensus latency` 和 `End-to-end latency` 之外，还输出 leader/非 leader 提交延迟、leader 间隔、非 leader 规则排序延迟、Rule 1/2/3 的 leader 和区块比例，以及已完成 ABA 节点实例的平均、最大和最小时长。测试结束时仍未决定的 ABA 不计入时长统计。

`faults > 0` 时使用主动敌手调度：所有 EC2 节点仍然启动。每轮根据 `ORCA_ADVERSARY_SEED`、轮次和委员会确定性伪随机选出 `f` 个敌手；敌手 leader 强制进入 Rule 3，非敌手 leader 正常检查 Rule 1/2。静默敌手不创建 Header，并暂停本地 batch 生产，但继续接收消息且 ABA 继续独立运行。默认种子为 `0`，例如 `ORCA_ADVERSARY_SEED=42 fab local` 可得到另一条可复现轨迹。ABA 在进入 `r+3` 前输出的结果归入 Rule 2，之后的结果归入 Rule 3。

若本轮随机敌手中包含 leader，可在运行 Fabric 前通过 `ORCA_RULE3_BEHAVIOR` 选择该强制 Rule 3 leader 的行为：`silent` 表示静默，`participate` 表示继续参与，`mixed`（默认）表示根据相同种子确定性选择静默或参与，两种结果概率各为 1/2。例如：`ORCA_RULE3_BEHAVIOR=silent fab local`。该参数会自动传入本地或远端 Primary。

本地 Fabric 示例：

```bash
# 默认：每个 Rule 3 leader 以 1/2 概率静默、1/2 概率参与
ORCA_RULE3_BEHAVIOR=mixed fab local

# Rule 3 leader 全部静默
ORCA_RULE3_BEHAVIOR=silent fab local

# 对照组：Rule 3 leader 全部参与
ORCA_RULE3_BEHAVIOR=participate fab local
```

Fabric 是否启用敌手调度仍由 `benchmark/fabfile.py` 中的 `faults` 决定；必须满足 `faults > 0`，上述 Rule 3 行为才会生效。

AWS 多服务器脚本从 Node 0 的 `ORCA_FAULTS` 和 `ORCA_RULE3_BEHAVIOR` 环境变量读取设置。`ORCA_FAULTS` 必须大于 0 才会启用敌手调度。例如：

```bash
cd ~/Orca-B
ORCA_FAULTS=1 ORCA_RULE3_BEHAVIOR=mixed ./run-multi-servers.sh 10 20 10000
ORCA_FAULTS=1 ORCA_RULE3_BEHAVIOR=silent ./run-multi-servers.sh 10 20 10000
ORCA_FAULTS=1 ORCA_RULE3_BEHAVIOR=participate ./run-multi-servers.sh 10 20 10000
```

## 6. 故障排查

- `hostname contains invalid characters`：hosts 文件只写纯 IP，不要写 `ubuntu@` 或空格。
- `NoneType ... group`：client 没有打印 `Start sending transactions`，先检查全部 3003 端口。
- `Malformed/Serialization`：确认所有机器 commit 完全相同并重启全部进程。
- RocksDB bindgen 报错：确认 clang-14 环境变量完整。
- ready=0/N：在 Node 0 执行 `while read ip; do nc -vz -w3 "$ip" 3003; done < deploy/hosts-N.txt`。
- ABA 无法推进：检查安全组已开放集群内部 TCP 3005，并执行 `while read ip; do nc -vz -w3 "$ip" 3005; done < deploy/hosts-N.txt`。
- 内存持续增长：检查 READY 验签是否长期低于输入速率；无界队列不会丢消息，但也不会自动限制内存。

测试完后停止或终止 EC2，并检查 EBS 卷、Elastic IP 和跨 AZ 流量费用。
