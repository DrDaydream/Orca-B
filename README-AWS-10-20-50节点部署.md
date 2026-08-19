# Orca-B：AWS EC2 10 / 20 / 50 节点完整部署

本文对应当前仓库的 `prepare-aws-cluster.sh` 与 `run-multi-servers.sh`。每台 EC2 运行一个 Primary、一个 Worker 和一个 benchmark client；node-0 同时作为控制机和第 0 个节点。Orca-B 的 ABA 使用独立 TCP 端口 3005，这是它与 Orca-A 部署最重要的区别。

## 1. 资源与参数

| 项目 | 值 |
|---|---|
| AMI | Ubuntu Server 24.04 LTS，x86_64 |
| 节点数 | 10、20 或 50 |
| 登录用户 | `ubuntu` |
| 项目目录 | `/home/ubuntu/Orca-B` |
| 仓库 | `https://github.com/DrDaydream/Orca-B.git` |
| 推荐实例 | 10 节点至少 4 vCPU / 16 GiB；20/50 节点建议 8 vCPU / 32 GiB |
| 磁盘 | 至少 30 GiB gp3 |
| 网络 | 同一 Region、同一 VPC，建议同一 AZ |
| 控制机 | node-0，同时参与协议 |

Orca-B 的 READY 工作队列无界，20/50 节点高压测试需监控内存。先用 10 节点、20 秒、10,000 总 TPS 跑通。协议要求 `n >= 3f+1`，建议最大敌手数为 10 节点 f=3、20 节点 f=6、50 节点 f=16。

## 2. AWS 控制台与安全组

1. AWS Console -> EC2 -> Security Groups -> Create security group。
2. 名称填写 `orca-b-sg`，选择实例所在 VPC。
3. 创建 ED25519 key pair `orca-b-aws.pem`。
4. Launch instances，选择 Ubuntu 24.04 x86_64、相同 VPC/子网/安全组，数量为 10、20 或 50。
5. 建议同一实例类型、同一 AZ、至少 30 GiB gp3。
6. 实例通过 2/2 status checks 后，按顺序命名 `orca-b-node-0` 至 `orca-b-node-N-1`。
7. 50 节点前在 Service Quotas 检查 On-Demand vCPU 配额。

入站规则：

| 协议/端口 | Source | 用途 |
|---|---|---|
| TCP 22 | 你的公网 IP /32 | 本地登录 |
| TCP 22 | `orca-b-sg` 自身 | node-0 私网 SSH |
| TCP 3000-3005 | `orca-b-sg` 自身 | Orca-B 内部协议 |

不要向 `0.0.0.0/0` 开放 3000-3005。

| 端口 | 用途 |
|---:|---|
| 3000 | Primary <-> Primary，GRBC/READY/同步 |
| 3001 | Worker -> Primary |
| 3002 | Primary -> Worker |
| 3003 | Client -> Worker |
| 3004 | Worker <-> Worker |
| 3005 | ABA <-> ABA 独立通道 |

## 2.1 五大洲跨 Region 部署

单 Region 基线可以使用安全组自身作为来源。五大洲实验建议在 5 个 Region 各放 2/4/10 台，对应 10/20/50 节点，例如 `us-east-1`、`sa-east-1`、`eu-west-2`、`ap-southeast-1`、`ap-southeast-2`。

为五个 VPC 使用不重叠 CIDR，例如 `10.10.0.0/16` 到 `10.50.0.0/16`。通过 AWS Cloud WAN 或 Transit Gateway inter-Region peering 建立私网连接，并在所有 route table 中配置双向路由。跨 Region 安全组不能只引用另一个 Region 的安全组名称；每个 Region 的入站规则应允许：

- TCP 22：你的公网 IP /32 和 node-0 VPC CIDR；
- TCP 3000-3005：全部五个集群 VPC CIDR，3005 不能遗漏；
- 出站：集群 CIDR、软件源和时间同步所需流量。

hosts、committee 的常规地址和 `aba_to_aba` 都必须填写私网可路由地址。node-0 必须能通过私网 SSH 到所有节点，并能访问每台的 3000-3005。固定公网 IP + /32 allowlist 可以作为无私网互联时的临时方案，但不建议，且跨 Region 流量会产生费用。记录每个 Region 的节点数、RTT 和实例类型，结果才可复现。


## 3. 配置 node-0 SSH

在本地电脑执行：

~~~bash
chmod 400 ~/Downloads/orca-b-aws.pem
scp -i ~/Downloads/orca-b-aws.pem ~/Downloads/orca-b-aws.pem \
  ubuntu@NODE0_PUBLIC_IP:/home/ubuntu/.ssh/orca-b-aws.pem
ssh -i ~/Downloads/orca-b-aws.pem ubuntu@NODE0_PUBLIC_IP
~~~

在 node-0 执行：

~~~bash
chmod 400 ~/.ssh/orca-b-aws.pem
nano ~/.ssh/config
~~~

写入：

~~~sshconfig
Host 10.*
    User ubuntu
    IdentityFile /home/ubuntu/.ssh/orca-b-aws.pem
    StrictHostKeyChecking accept-new
    ConnectTimeout 8
    ServerAliveInterval 5
    ServerAliveCountMax 2
~~~

若 VPC 私网不是 `10.*`，改为实际网段或 `Host *`。然后：

~~~bash
chmod 600 ~/.ssh/config
git clone https://github.com/DrDaydream/Orca-B.git ~/Orca-B
cd ~/Orca-B
cp deploy/hosts-10.txt.example deploy/hosts-10.txt 2>/dev/null || touch deploy/hosts-10.txt
nano deploy/hosts-10.txt
~~~

每行只写一个 Private IPv4，node-0 必须是第一行。20/50 节点分别使用 `deploy/hosts-20.txt`、`deploy/hosts-50.txt`。

~~~bash
wc -l deploy/hosts-10.txt
sort deploy/hosts-10.txt | uniq -d
while read -r ip; do ssh "$ip" hostname; done < deploy/hosts-10.txt
~~~

必须分别得到 10 行、无重复输出、所有节点都能返回 hostname。

## 4. 安装并编译全部节点

在 node-0 的 `~/Orca-B` 中执行，20/50 节点替换 hosts 文件：

~~~bash
while read -r ip; do
  ssh "$ip" 'bash -s' <<'REMOTE' &
set -Eeuo pipefail
sudo apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
  build-essential cmake clang-14 libclang-14-dev git curl tmux jq \
  python3 python3-pip netcat-openbsd chrony
sudo systemctl enable --now chrony
if [[ ! -x "$HOME/.cargo/bin/cargo" ]]; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
fi
source "$HOME/.cargo/env"
rustup default stable
if [[ -d "$HOME/Orca-B/.git" ]]; then
  git -C "$HOME/Orca-B" pull --ff-only
else
  git clone https://github.com/DrDaydream/Orca-B.git "$HOME/Orca-B"
fi
cd "$HOME/Orca-B"
LIBCLANG_PATH=/usr/lib/llvm-14/lib \
CLANG_PATH=/usr/bin/clang-14 \
CC=/usr/bin/clang-14 \
CXX=/usr/bin/clang++-14 \
CXXFLAGS='-include cstdint' \
cargo build --release --features benchmark
test -x target/release/node
test -x target/release/benchmark_client
REMOTE
done < deploy/hosts-10.txt
wait
~~~

首次 RocksDB 编译会较久。确认所有机器版本相同：

~~~bash
while read -r ip; do
  ssh "$ip" 'git -C ~/Orca-B rev-parse HEAD'
done < deploy/hosts-10.txt
~~~

## 5. 生成并分发配置

仓库脚本会生成 N 个密钥、3000-3005 committee、`max_header_delay=200` 的参数文件，并分发到各节点：

~~~bash
cd ~/Orca-B
chmod +x prepare-aws-cluster.sh
HOSTS_FILE=deploy/hosts-10.txt ./prepare-aws-cluster.sh 10
~~~

20/50 节点：

~~~bash
HOSTS_FILE=deploy/hosts-20.txt ./prepare-aws-cluster.sh 20
HOSTS_FILE=deploy/hosts-50.txt ./prepare-aws-cluster.sh 50
~~~

脚本依赖 `~/.ssh/config`，也可覆盖路径：

~~~bash
REMOTE_USER=ubuntu \
REMOTE_DIR=/home/ubuntu/Orca-B \
HOSTS_FILE=/home/ubuntu/Orca-B/deploy/hosts-10.txt \
./prepare-aws-cluster.sh 10
~~~

每次更改节点规模都必须重新运行。每台只接收自己的 `node-i.json`，所有节点的 `committee.json` 和 `parameters.json` 必须相同。检查：

~~~bash
while read -r ip; do
  ssh "$ip" 'sha256sum ~/Orca-B/deploy/committee.json'
done < deploy/hosts-10.txt
~~~

## 6. 敌手与 ABA 选项

| 环境变量 | 默认值 | 含义 |
|---|---|---|
| `ORCA_FAULTS` | `0` | 每轮敌手数；0 为无敌手 |
| `ORCA_ADVERSARY_SEED` | `0` | 确定性随机种子 |
| `ORCA_RULE3_BEHAVIOR` | `mixed` | `mixed`、`silent` 或 `participate` |
| `ORCA_CLIENT_DURING_SILENCE` | `send` | `send` 保持输入；`pause` 按时序表暂停 |
| `ORCA_CLIENT_SILENCE_SLOT_MS` | `max_header_delay` | 静默时间槽毫秒数 |

当 `ORCA_FAULTS>0` 时，每轮按种子选择 f 个敌手。敌手 leader 强制进入 Rule 3；`mixed` 确定性地选择静默或参与，`silent` 始终静默，`participate` 始终参与。非敌手 leader 的 Rule 1/Rule 2 长期约 1:1，但比例以所有 leader 为分母。

ABA 独立运行：静默 Primary 不创建 Header，但继续接收协议消息，ABA 仍可处理输入输出。进入 `r+3` 前完成的 ABA 结果计入 Rule 2，之后计入 Rule 3。缺少本地 leader 不等于 ABA 不能输入 1。安全组缺少 TCP 3005 会造成 ABA 停滞。

`pause` 使用 benchmark 运行前生成的单向墙钟时间表，不使用 Worker -> Client 反馈；`send` 保持交易流量，便于区分协议静默与输入下降的影响。

## 7. 运行 10 / 20 / 50 节点

参数为节点数、正式运行秒数、集群总 TPS；TPS 会均摊到全部 Client。

无敌手基线：

~~~bash
cd ~/Orca-B
chmod +x run-multi-servers.sh
./run-multi-servers.sh 10 20 10000
./run-multi-servers.sh 20 60 10000
./run-multi-servers.sh 50 60 10000
~~~

推荐最大容错敌手测试：

~~~bash
ORCA_FAULTS=3 ORCA_ADVERSARY_SEED=42 \
ORCA_RULE3_BEHAVIOR=mixed ORCA_CLIENT_DURING_SILENCE=pause \
./run-multi-servers.sh 10 20 10000

ORCA_FAULTS=6 ORCA_ADVERSARY_SEED=42 \
ORCA_RULE3_BEHAVIOR=mixed ORCA_CLIENT_DURING_SILENCE=pause \
./run-multi-servers.sh 20 60 10000

ORCA_FAULTS=16 ORCA_ADVERSARY_SEED=42 \
ORCA_RULE3_BEHAVIOR=mixed ORCA_CLIENT_DURING_SILENCE=pause \
./run-multi-servers.sh 50 60 10000
~~~

对照组：

~~~bash
# 敌手 leader 静默，但 Client 仍发送
ORCA_FAULTS=3 ORCA_RULE3_BEHAVIOR=silent \
ORCA_CLIENT_DURING_SILENCE=send \
./run-multi-servers.sh 10 20 10000

# 敌手 leader 继续参与
ORCA_FAULTS=3 ORCA_RULE3_BEHAVIOR=participate \
ORCA_CLIENT_DURING_SILENCE=send \
./run-multi-servers.sh 10 20 10000
~~~

自定义路径：

~~~bash
REMOTE_USER=ubuntu \
REMOTE_DIR=/home/ubuntu/Orca-B \
HOSTS_FILE=/home/ubuntu/Orca-B/deploy/hosts-10.txt \
./run-multi-servers.sh 10 20 10000
~~~

脚本等待全部 Worker 和 Client 就绪后计时，结束时下载日志到 `benchmark/logs/` 并输出 TPS、延迟、Rule 1/2/3 比例及已完成 ABA 实例的平均/最大/最小时长。未完成 ABA 不计入时长统计。

## 8. 运行前检查

~~~bash
# 所有版本相同
while read -r ip; do ssh "$ip" 'git -C ~/Orca-B rev-parse HEAD'; done < deploy/hosts-10.txt

# 测试运行期间检查 Worker 和 ABA 端口
while read -r ip; do
  nc -vz -w 2 "$ip" 3003
  nc -vz -w 2 "$ip" 3005
done < deploy/hosts-10.txt

# 时间、资源
while read -r ip; do
  ssh "$ip" 'chronyc tracking | head -5; nproc; free -h; df -h /'
done < deploy/hosts-10.txt
~~~

## 9. 排障

- `hostname contains invalid characters`：hosts 只能写纯私网 IP。
- `ready=0/N`：检查全部 Worker 的 3003 和 `run/logs/worker-*-0.log`。
- `NoneType object has no attribute group`：至少一个 Client 未打印 `Start sending transactions`。
- ABA 无法推进：检查安全组自身 TCP 3005、committee 的 `aba_to_aba` 地址以及 Primary 日志。
- `librocksdb-sys` / bindgen 报错：使用 clang-14 的完整编译环境变量。
- `Malformed` / `Serialization`：所有机器必须使用同一 commit 和 committee。
- 全 0：检查测试是否真正开始、Primary 是否提交、运行时间是否过短。
- 内存持续增长：降低总 TPS，查看 READY/ABA 队列积压。
- `Connection refused`：进程未监听；`timed out`：通常是安全组、NACL、UFW 或地址错误。

~~~bash
ssh NODE_PRIVATE_IP 'tail -100 ~/Orca-B/run/logs/primary-INDEX.log'
ssh NODE_PRIVATE_IP 'tail -100 ~/Orca-B/run/logs/worker-INDEX-0.log'
ssh NODE_PRIVATE_IP 'tail -100 ~/Orca-B/run/logs/client-INDEX-0.log'
~~~

同一实验对比必须固定硬件、节点规模、总 TPS、持续时间、参数和种子。测试后停止或终止 EC2，并检查 EBS、Elastic IP、公网 IPv4 与跨 AZ 流量费用。不要把 pem 或 `deploy/node-*.json` 上传到 GitHub。
