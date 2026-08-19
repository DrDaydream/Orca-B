# Windows 使用五个 Region PEM 控制 Orca-B

本文适用于 Windows 本地保存五个区域 PEM、Linux 用户为 `ubuntu`、节点分布在五个 AWS Region、`node-0` 兼任控制机的部署。五个 PEM 对应 Region，不对应协议；四种协议可以共用同一套 SSH 配置。

Orca-B 除 TCP 3000-3004 外，还必须允许节点私网之间访问 ABA TCP 3005。

## 1. Windows 登录 node-0

假设 node-0 在欧洲，使用欧洲 PEM：

~~~powershell
ssh -V
scp -V
$Node0Pem = "C:\Users\YOUR_NAME\Downloads\eu-west-2.pem"
icacls $Node0Pem /inheritance:r
icacls $Node0Pem /grant:r "$($env:USERNAME):(R)"
ssh -i $Node0Pem ubuntu@NODE0_PUBLIC_IP
~~~

## 2. 上传五个 PEM

~~~powershell
$Node0Pem = "C:\Users\YOUR_NAME\Downloads\eu-west-2.pem"
$PemDir = "C:\Users\YOUR_NAME\Downloads"
ssh -i $Node0Pem ubuntu@NODE0_PUBLIC_IP "mkdir -p /home/ubuntu/.ssh && chmod 700 /home/ubuntu/.ssh"
scp -i $Node0Pem "$PemDir\us-east-1.pem" ubuntu@NODE0_PUBLIC_IP:/home/ubuntu/.ssh/
scp -i $Node0Pem "$PemDir\sa-east-1.pem" ubuntu@NODE0_PUBLIC_IP:/home/ubuntu/.ssh/
scp -i $Node0Pem "$PemDir\eu-west-2.pem" ubuntu@NODE0_PUBLIC_IP:/home/ubuntu/.ssh/
scp -i $Node0Pem "$PemDir\ap-southeast-1.pem" ubuntu@NODE0_PUBLIC_IP:/home/ubuntu/.ssh/
scp -i $Node0Pem "$PemDir\ap-southeast-2.pem" ubuntu@NODE0_PUBLIC_IP:/home/ubuntu/.ssh/
ssh -i $Node0Pem ubuntu@NODE0_PUBLIC_IP
~~~

不要把 PEM 提交到 GitHub。

## 3. node-0 SSH config

~~~bash
chmod 400 /home/ubuntu/.ssh/*.pem
nano /home/ubuntu/.ssh/config
~~~

根据实际 VPC CIDR写入：

~~~sshconfig
Host 10.10.*
    User ubuntu
    IdentityFile /home/ubuntu/.ssh/us-east-1.pem

Host 10.20.*
    User ubuntu
    IdentityFile /home/ubuntu/.ssh/sa-east-1.pem

Host 10.30.*
    User ubuntu
    IdentityFile /home/ubuntu/.ssh/eu-west-2.pem

Host 10.40.*
    User ubuntu
    IdentityFile /home/ubuntu/.ssh/ap-southeast-1.pem

Host 10.50.*
    User ubuntu
    IdentityFile /home/ubuntu/.ssh/ap-southeast-2.pem

Host 10.*
    StrictHostKeyChecking accept-new
    ConnectTimeout 8
    ServerAliveInterval 5
    ServerAliveCountMax 2
~~~

~~~bash
chmod 600 /home/ubuntu/.ssh/config
ssh -G 10.10.1.10 | grep -E '^(user|identityfile) '
ssh ubuntu@NODE_IN_EACH_REGION_PRIVATE_IP hostname
~~~

## 4. hosts 文件

~~~bash
git clone https://github.com/DrDaydream/Orca-B.git /home/ubuntu/Orca-B
cd /home/ubuntu/Orca-B
cp deploy/hosts-10.txt.example deploy/hosts-10.txt 2>/dev/null || touch deploy/hosts-10.txt
nano deploy/hosts-10.txt
~~~

每行一个私网 IPv4，第一行是 node-0。例如五个 Region 各两个节点：

~~~text
10.30.1.10
10.30.1.11
10.10.1.10
10.10.1.11
10.20.1.10
10.20.1.11
10.40.1.10
10.40.1.11
10.50.1.10
10.50.1.11
~~~

不要写 `ubuntu@`、公网 IP、端口或主机名。

~~~bash
while read -r ip; do ssh ubuntu@"$ip" hostname; done < deploy/hosts-10.txt
~~~

## 5. 准备和运行 Orca-B

按照 [AWS 完整部署文档](README-AWS-10-20-50节点部署.md) 安装并编译。脚本通过 `~/.ssh/config` 按 IP 选择 PEM，不要传单个 `SSH_KEY`：

~~~bash
cd /home/ubuntu/Orca-B
REMOTE_USER=ubuntu \
REMOTE_DIR=/home/ubuntu/Orca-B \
HOSTS_FILE=/home/ubuntu/Orca-B/deploy/hosts-10.txt \
./prepare-aws-cluster.sh 10

REMOTE_USER=ubuntu \
REMOTE_DIR=/home/ubuntu/Orca-B \
HOSTS_FILE=/home/ubuntu/Orca-B/deploy/hosts-10.txt \
./run-multi-servers.sh 10 20 10000
~~~

运行前从 node-0 检查所有节点 TCP 3000-3005 的私网路由、安全组和 NACL。

