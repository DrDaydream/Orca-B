# 最终提交延迟统计

本次改动仅添加观测日志与离线统计。Fabric 任务、安装与运行脚本、实验参数、
交易内容、负载节奏和协议的判定、发送、重试、提交顺序不变。
统计基于客户端原有的采样交易（每个非空活动 burst 一笔），不是对全部交易逐笔计时。

## 两个指标及计时边界

主指标为 `release_to_final`：负载实际释放 → 最终有序提交。
辅助指标为 `ingress_to_final`：协议入口接收 → 最终有序提交。
同时输出 `release_to_ingress`，逐笔计算入口前开销。不能用两个 P95 相减得到它。

`Benchmark sample released id … at_us …` 在现有客户端每个非空活动 burst 的
timer tick（及 Orca 的 silence 判断）之后、第一次发送或惰性 RPC 流构造之前记录。
因此该 burst 中采样交易等待前面交易发送、传输背压、RPC 流消费及网络传输的时间
均包含在主指标中。它是实际 burst 释放时间，不是预定计划时间：原客户端发送循环
受阻时，尚未开始的下一 burst 并未独立释放。这次没有引入独立开环生成器或改变负载逻辑。

`Benchmark sample ingress id … at_us …` 在 worker 收到完整应用层交易之后、
向 batch maker 的 channel 发送及其排队之前记录。Orca 使用 TCP 帧 handler；
Bullshark 使用 unary RPC handler 或 streaming RPC 的单笔交易读取点。
此前的 socket/RPC 缓冲等待计入入口前延迟；入口后的内部排队、打包和传播计入辅助指标。

最终提交采用每个 primary 的最终有序提交事件。Orca 优先读取 `commit_us`，
兼容 `commit` 毫秒字段；`@` 字段只是规则就绪时间，不用作本指标的终点。
Bullshark 读取既有 `Committed` 事件的时间戳。
Orca 沿用既有最终提交决定的计时位置：有序 sequence 已确定、调用 `state.update` 之前；
不包含后续状态更新、提交输出队列等待、应用执行或客户端确认。Bullshark 同样以共识层
确定提交序列的既有日志为终点，不等待执行器完成。
新 Orca 日志的两个精度来自同一次时钟读取；旧毫秒日志存在取整精度限制。

## 输出与使用

现有 `SUMMARY` 的 `End-to-end latency` 改为主指标的平均值。
旧口径单独列为 `Legacy send-based latency`；既有 TPS/BPS 统计未在本次重定义。
新 `FINAL COMMIT MEASUREMENTS` 区域给出三个延迟分布的 mean、P50、P95、P99。
`LogParser.print(filename)` 在原文本文件旁追加 `filename.metrics.jsonl`，
每次调用一行 JSON，含各副本的计数、分布和日志文件名映射。
Orca 已有的 `comparison.json` 额外包含 `measurement_stats`；旧字段保留其原定义。
旧字段的 first-replica 口径不能替代 `measurement_stats` 的主指标。

不修改运行脚本也可以在离线解析时指定测量窗口。在 `benchmark/` 目录中：

```python
from benchmark.logs import LogParser

parser = LogParser.process('logs', faults=0)
metrics = parser.measurement_metrics(duration=300, warmup=60, drain=30)
```

窗口以最早客户端 `Start sending transactions` 为基准，先跳过 `warmup` 秒，
纳入之后 `duration` 秒内实际释放的采样交易；`drain` 只扩展最终提交的观察截止。
这些参数仅过滤已有日志，不会延长运行或产生缺失数据。
未显式指定 duration 的常规 SUMMARY 使用完整的已观察日志跨度，标记为
`observed_log_span`，不将最后一次提交当作实验结束。配置窗口标记为
`configured_duration`；`last_observed_log_us` 便于核对日志是否覆盖了请求的窗口。
应在日志确实覆盖测量与 drain 时间时再报告论文结果。

交易 ID 由“client/worker 日志对 + 现有 sample counter”组成；文件后缀不匹配即拒绝解析。
重试/重复日志取同一 ID 的首次释放、首次入口接收，以及每副本的首次最终提交。
同一批次里的不同采样交易仍各占一条观测。

三个分布只使用该副本上同时具有 release、ingress、final 且时间顺序有效的相同样本，
确保逐笔满足主延迟 = 入口前延迟 + 辅助延迟。
保留每副本统计；总体分布由原始 transaction-replica 样本合并得到，
不是副本 P95 的平均，也不是所有副本中最早提交的一次。
它对完成的 transaction-replica 样本加权，不对副本等权。

每副本分别报告已释放、已完成、未完成、未接收、缺入口时间以及时间顺序异常的样本数。
完成率的分母包含未完成交易；延迟分布则是有效的已完成配对样本分布。
缺埋点的旧日志显示 `unavailable` / `N/A` / JSON `null`，不回退成零延迟；
部分来源有埋点时标记 `partial`。没有新埋点的已有二进制不能产生新指标。
统计聚合遇到 N/A 会明确拒绝聚合，不制造零延迟。

跨机器延迟使用 Unix wall-clock，要求节点时钟对齐。负延迟或不符合
release ≤ ingress ≤ commit 的样本被计入 `invalid_time_samples`，不截断成零。
本次未改动时间同步配置，也未执行编译或集群实验。

## Orca-B 的 ABA 通信开销

仅 Orca-B 新增专用 ABA 通道的字节计数。日志格式为：

```text
Benchmark ABA bytes at_us … elapsed_us … sent … received …
```

发送计数在每个接收方的完整 `writer.send` 成功后增加，包含成功的应用层重传。
计入签名消息的完整序列化 envelope（作者、payload、签名等）与 4 字节帧长前缀。
接收计数在完整帧交给 ABA handler 时增加，在解码与验证前计数，所以包含重复帧和
完整但验证失败的帧。不包含本地 self-delivery、ACK、TCP/IP 首部、底层 TCP 重传、
失败写入的部分字节或未形成完整帧的接收数据。发送成功表示交给传输层，非对端确认。

启动时记录零基线，此后约每秒记录累计值，包括无 ABA 流量的空闲时间。
每副本发送速率为 `Δsent / (Δelapsed_us / 1_000_000)`，接收速率同理，单位均为
**byte/s**。分母使用 monotonic elapsed，不使用 ABA 活跃时长，也不使用最后提交时间。
窗口边缘只取范围内完整的首尾快照，不插值；实际快照起止时间与覆盖秒数写入结果。
最后快照之后的未采样尾部不纳入速率。两个有效快照之间零增量表示 0 byte/s；
无埋点或不足两个快照表示不可用。计数回退或 wall-clock 倒退被明确标记。

总体开销主看各副本发送速率及其和、平均值。JSON 同时保留接收速率作为核对。
不要把发送与接收相加作为唯一网络开销，因为同一传输会在两端重复计数。
`sum_replica_sent_bytes_per_second` 是各自已报告观察窗口的速率之和；
跨节点快照边界可能略有不同，不能宣称它是完全同步窗口的逐包计数。

现有 ABA 耗时统计保持可用，单位仍为 ms，与本次通信开销 byte/s 分开报告。
