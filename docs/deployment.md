# 部署

生产系统分为两个明确的部署目标：

```text
Vultr edge
  Binance -> collector -> Parquet/Zstd raw -> restricted SFTP

Campus 107
  minute cron pull -> persistent raw -> Slurm normalize/L2/finalize -> derived
```

每台机器都有独立且自包含的部署入口：

- Vultr 安装、SFTP 限制、启动、检查和升级：
  [`deploy/vultr/README.md`](../deploy/vultr/README.md)
- 校园 107 的 SIF、拉取 cron、Slurm 处理、检查和升级：
  [`deploy/campus-107/README.md`](../deploy/campus-107/README.md)

发布 tag 会生成 Vultr 使用的不可变 OCI image digest，以及校园使用的 SIF 和 SHA-256。
不要从 `main` 临时构建生产版本，也不要在两台机器之间手工复制未校验的 Python 源码。

生产前必须获得管理员对 login node 每分钟执行轻量 SFTP pull 的书面许可。pull 只执行
下载、hash、fsync、rename 和 ACK；解析与 Parquet 处理全部进入 Slurm。

Slurm 的 normalize、L2 array 和 finalize 依赖关系由校园部署目录中的 `submit-day.sh`
统一生成。脚本根据 symbol 文件计算 array 上限，避免手工填写 `0-59` 一类容易出错的值。

## Canary

按 20、40、50、60 instruments 每级至少运行 24 小时，最后一级运行 72 小时。依据日志
检查 CPU、CPU steal、RSS、Arrow allocator、queue、event-loop lag、compressed bytes 和
finalize latency。只有下式能在 25GB SSD 的可用空间内成立时才接受该磁盘：

```text
required_spool = 1.5 * max_rolling_6h_compressed_bytes
```

1C1G 失败则升级到 2C2G；磁盘失败则扩盘，不通过降低数据保真度过关。
