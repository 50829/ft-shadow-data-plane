# 部署

## Edge

目标是 Ubuntu 24.04、Docker、1C1G、25GB SSD。先创建与容器 UID 一致的受限 SFTP 用户
和目录；以下命令需要 root，正式执行前核对 UID 未被占用：

```bash
useradd --uid 10001 --home-dir / --no-create-home --shell /usr/sbin/nologin data-puller
install -d -o root -g root -m 755 /srv/ft-data-sftp
install -d -o 10001 -g 10001 -m 750 \
  /srv/ft-data-sftp/ready \
  /srv/ft-data-sftp/writing \
  /srv/ft-data-sftp/control \
  /srv/ft-data-sftp/control/acks \
  /srv/ft-data-sftp/control/universe/inbox
```

`sshd_config` 为该账户追加：

```text
Match User data-puller
    ChrootDirectory /srv/ft-data-sftp
    ForceCommand internal-sftp
    PasswordAuthentication no
    DisableForwarding yes
    PermitTunnel no
```

仅安装校园 puller 的 SSH public key，并在防火墙允许校园出口 IP 访问 SSH。采集器不发布
任何 TCP 端口。将 `edge.yaml` 的 `data_root` 设置为 `/data`，再安装 compose、systemd unit
和固定 digest 的 `edge.env`。镜像升级只修改 digest 后重启 unit，接受短暂 planned gap。

`alert.env` 只包含 `ALERT_EMAIL=...`；主机必须已经配置可用的 `mail`/MTA。

## Campus Pull

生产前必须获得管理员对以下行为的书面许可：login node 每分钟运行一次轻量 SFTP pull，
只做下载、hash、fsync、rename 和 ACK；解析与 Parquet 处理全部进入 Slurm。

把 release SIF、`central.yaml` 和 `crontab.example` 放到 persistent user directory。首次连接
需人工核对并固定 edge SSH host key，禁止使用 `AutoAddPolicy` 或关闭 host-key 检查。

## Slurm

先提交 normalize，成功后提交最多 16 并发的 symbol array，最后以依赖关系提交 finalize：

```bash
normalize_job=$(sbatch --parsable deploy/slurm/normalize.sbatch)
l2_job=$(sbatch --parsable --dependency=afterok:$normalize_job --array=0-59%16 deploy/slurm/l2-array.sbatch)
sbatch --dependency=afterok:$l2_job deploy/slurm/finalize.sbatch
```

数组上限必须等于 `symbols` 文件行数减一，不能机械使用示例中的 `59`。

## Canary

按 20、40、50、60 instruments 每级至少运行 24 小时，最后一级运行 72 小时。依据日志
检查 CPU、CPU steal、RSS、Arrow allocator、queue、event-loop lag、compressed bytes 和
finalize latency。只有下式能在 25GB SSD 的可用空间内成立时才接受该磁盘：

```text
required_spool = 1.5 * max_rolling_6h_compressed_bytes
```

1C1G 失败则升级到 2C2G；磁盘失败则扩盘，不通过降低数据保真度过关。
