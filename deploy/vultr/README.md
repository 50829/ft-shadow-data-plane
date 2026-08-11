# Vultr 正式采集部署

本手册适用于 `167.179.115.243` 上的 v0.2 collector。数据根为
`/srv/ft-data-rsync`，collector 和受限传输账户都使用 UID/GID 10001。

## 1. 前置条件

以 root 安装 Docker Engine、Compose plugin、OpenSSH、rsync 和 rrsync，并确认系统时钟同步：

```bash
docker version
docker compose version
rsync --version
rrsync -h
timedatectl status
```

防火墙只需允许 SSH 管理来源和 107 的出口地址。collector 只向 Binance 发起出站 HTTPS/WSS。

## 2. 安装目录和服务

在 v0.2 仓库根目录执行：

```bash
sudo ./deploy/vultr/install.sh
```

安装器创建：

```text
/srv/ft-data-rsync/ready
/srv/ft-data-rsync/writing
/srv/ft-data-rsync/control/acks
/srv/ft-data-rsync/control/universe
/etc/ft-shadow-data-plane/edge.yaml
/etc/ft-shadow-data-plane/edge.env
/opt/ft-shadow-data-plane/deploy/vultr
```

它不会启动 collector，也不会改写已经存在的配置。

## 3. 配置受限 rsync

把 107 的 `~/.ssh/ft-data-puller.pub` 放到 Vultr 的临时管理路径，然后执行：

```bash
sudo /opt/ft-shadow-data-plane/deploy/vultr/configure-rsync.sh \
  /root/ft-data-puller.pub
```

脚本把 key 安装到 root 管理的 `AuthorizedKeysFile`，强制执行：

```text
/usr/bin/rrsync -no-del -no-overwrite /srv/ft-data-rsync
```

该 key 没有交互 shell、TTY、端口转发或 X11 权限，不能要求服务端删除或覆盖文件。不要为
该账户叠加其他 `ForceCommand` 或 chroot，它们会阻止 rsync 的远端进程。升级时，脚本会
识别并禁用旧版仓库生成的 `ft-data-puller.conf`；若发现其他冲突配置，它会在 reload 前失败。

显示并通过独立渠道发给 107 操作者核对 host key：

```bash
ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub
```

## 4. 配置正式 60 币和镜像

`/etc/ft-shadow-data-plane/edge.yaml` 必须使用仓库 v0.2 示例。核对三个角色为 50/5/5，
`automation_enabled: true`，public shards 为 2，queue 为 64MiB。不要加入旧字段。

在 `/etc/ft-shadow-data-plane/edge.env` 中写 immutable digest：

```text
EDGE_IMAGE=ghcr.io/50829/ft-shadow-data-plane@sha256:<release-digest>
EDGE_DATA_ROOT=/srv/ft-data-rsync
EDGE_CONFIG=/etc/ft-shadow-data-plane/edge.yaml
```

拉取并检查架构：

```bash
set -a
. /etc/ft-shadow-data-plane/edge.env
set +a
docker pull "$EDGE_IMAGE"
docker image inspect "$EDGE_IMAGE" --format '{{json .RepoDigests}}'
```

Compose 已固定 0.90 CPU、768MiB RAM、256 PIDs、只读 rootfs 和日志轮换。

## 5. v0.2 clean start

只有在确认旧数据无需保留时执行。以下删除不可恢复，目标必须逐项等于显示值：

```bash
sudo systemctl stop ft-shadow-data-plane.service || true
for path in \
  /srv/ft-data-rsync/ready \
  /srv/ft-data-rsync/writing \
  /srv/ft-data-rsync/control
do
  readlink -f "$path"
done
```

人工核对输出后，删除且只删除这三个目录，再重新运行 installer：

```bash
sudo rm -rf -- \
  /srv/ft-data-rsync/ready \
  /srv/ft-data-rsync/writing \
  /srv/ft-data-rsync/control
sudo ./deploy/vultr/install.sh
```

## 6. 验证和启动

首次启动前，107 必须已能执行 `rsync --list-only`。然后：

```bash
sudo systemctl enable ft-shadow-data-plane.service
sudo systemctl start ft-shadow-data-plane.service
sudo /opt/ft-shadow-data-plane/deploy/vultr/verify.sh
```

观察启动：

```bash
journalctl -u ft-shadow-data-plane.service -f
```

只有出现以下日志后才进入正式时间范围：

```text
FORMAL_COLLECTION_STARTED ... generation=1 symbols=60
```

如果两次状态请求确认初始名单中有非交易合约，collector 会拒绝写正式标记并退出。此时更新
`edge.yaml` 的 50/5/5 角色，重新执行 clean start；不要绕过检查或减少总数。

同时确认：

```bash
sudo test -s /srv/ft-data-rsync/control/formal-start.json
sudo jq '{generation,core,boundary,probe,universe_hash}' \
  /srv/ft-data-rsync/control/universe/active.json
sudo find /srv/ft-data-rsync/ready -type f | head
```

## 7. 日常检查

```bash
systemctl status ft-shadow-data-plane.service
docker stats --no-stream
docker inspect ft-shadow-data-plane-collector-1 \
  --format 'oom={{.State.OOMKilled}} restarts={{.RestartCount}}'
df -h /srv/ft-data-rsync
find /srv/ft-data-rsync/ready -type f | wc -l
find /srv/ft-data-rsync/control/acks -type f | wc -l
journalctl -u ft-shadow-data-plane.service --since '24 hours ago' \
  | grep -E 'GAP|collector status|FORMAL_COLLECTION_STARTED|planned universe'
```

`control/universe/observations` 保存每日证据，`evaluations` 保存每次评估，`decisions` 保存实际
generation。正常日切没有成员变化时不会出现计划 gap。若发生替换，gap 只应列出移除和新增币。

暂停自动选币时，把 `automation_enabled` 改为 `false` 并重启 collector；采集仍继续，core
不能通过手工 override 直接修改。

## 8. 24 小时性能验收

每分钟 collector status 日志包含 RSS、Arrow bytes、CPU time、steal、event-loop lag、queue
ratio、writer idle 和 finalize 时间。按照 [实施合同](../../docs/implementation-plan.md) 计算
p95/p99。若 OOM、RSS 峰值超过 700MiB、CPU p95 超过 80%、queue 连续过高、磁盘低于 5GiB
或出现性能 gap，不得通过减少 60 币或降低频率规避；应先停止并扩容或优化。

## 9. 升级

先在 107 安装相同 release，再更新 Vultr digest。停止服务、备份配置文件、重新运行 installer
和 verify，再启动。v0.2 内升级保留 `control/universe` 与未 ACK spool；只有明确执行 clean start
才删除它们。
