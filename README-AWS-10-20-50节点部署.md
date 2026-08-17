# Orca-B AWS EC2 10/20/50 节点部署

每台 EC2 运行一个 Primary、一个 Worker 和一个 benchmark client。Node 0 同时作为控制机。所有协议通信必须使用私网 IP。

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
| 3000–3004/TCP | 安全组自身 | Orca-B 私网通信 |

不要将 3000–3004 开放给 `0.0.0.0/0`。

| 端口 | 用途 |
|---:|---|
| 3000 | Primary ↔ Primary（GRBC + ABA Bundle） |
| 3001 | Worker → Primary |
| 3002 | Primary → Worker |
| 3003 | Client → Worker |
| 3004 | Worker ↔ Worker |

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

**所有机器必须运行完全相同的 Git commit。** Orca-B 新增了 `PrimaryMessage::Bundle`，新旧二进制混用会导致网络消息反序列化失败。检查：

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

脚本根据 hosts 文件生成 `deploy/committee.json`：每个 authority 的端口是对应私网 IP 的 3000–3004，stake 为 1，worker id 为 0，并校验所有服务器上的 committee SHA-256。

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

当前结果只输出 `Consensus latency` 和 `End-to-end latency`；内部日志仍保留 commit timestamp，但不在汇总中显示 commit latency。

## 6. 故障排查

- `hostname contains invalid characters`：hosts 文件只写纯 IP，不要写 `ubuntu@` 或空格。
- `NoneType ... group`：client 没有打印 `Start sending transactions`，先检查全部 3003 端口。
- `Malformed/Serialization`：确认所有机器 commit 完全相同并重启全部进程。
- RocksDB bindgen 报错：确认 clang-14 环境变量完整。
- ready=0/N：在 Node 0 执行 `while read ip; do nc -vz -w3 "$ip" 3003; done < deploy/hosts-N.txt`。
- 内存持续增长：检查 READY 验签是否长期低于输入速率；无界队列不会丢消息，但也不会自动限制内存。

测试完后停止或终止 EC2，并检查 EBS 卷、Elastic IP 和跨 AZ 流量费用。
