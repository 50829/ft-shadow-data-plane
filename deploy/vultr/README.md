# Vultr 边缘采集机

这个目录是 Vultr 机器唯一需要阅读的部署入口。该机器通过 Docker 运行 Binance 采集器，
把 READY chunk 保存在 `/srv/ft-data-sftp`，并只允许校园 puller 通过受限 SFTP 访问。

## 1. 前置条件

- Ubuntu 24.04，已经安装 Docker Engine 和 Compose plugin；
- 检出本仓库的固定 release tag；
- 校园 puller 的 SSH 公钥；
- Release 页面 `edge-image.txt` 中的不可变 OCI image 地址；
- 正常的时间同步；如需邮件告警，还要有可用的 `mail` 命令和 MTA。

初始 1C1G、25GB 只是 canary 候选配置，不是最终容量结论。

## 2. 安装文件和目录

在仓库 checkout 根目录执行：

```bash
sudo ./deploy/vultr/install.sh
```

安装器会创建 UID/GID 为 10001 的 `data-puller` 账户和 SFTP 数据目录，把部署文件安装到
`/opt/ft-shadow-data-plane/deploy/vultr`，并在 `/etc/ft-shadow-data-plane` 中首次创建
本机配置。它不会启动采集器，也不会修改 SSH 或防火墙。

## 3. 配置采集器

编辑以下文件：

```text
/etc/ft-shadow-data-plane/edge.env
/etc/ft-shadow-data-plane/edge.yaml
/etc/ft-shadow-data-plane/alert.env
```

把 `EDGE_IMAGE` 设置为 `edge-image.txt` 中完整的 `ghcr.io/...@sha256:...` 地址。
`edge.yaml` 中保持 `data_root: /data`；Compose 会把它映射到主机的
`/srv/ft-data-sftp`。仓库中的示例已经固定为
`universe/bootstrap-2026-08-10T120017Z/stage-20.members.txt` 对应的首次 canary 名单；不要在
部署时重新排名。根据需要修改告警邮箱。

## 4. 限制 SFTP

把校园公钥放到 root 管理的目录，并为 `data-puller` 配置 chroot 和仅 SFTP 权限。一种
可用配置如下：

```text
Match User data-puller
    ChrootDirectory /srv/ft-data-sftp
    ForceCommand internal-sftp
    AuthorizedKeysFile /etc/ssh/authorized_keys/%u
    PasswordAuthentication no
    DisableForwarding yes
    PermitTunnel no
```

重新加载 SSH 前，先检查公钥权限和完整配置：

```bash
sudo install -d -o root -g root -m 755 /etc/ssh/authorized_keys
sudo install -o root -g root -m 600 campus-107.pub \
  /etc/ssh/authorized_keys/data-puller
sudo sshd -t
sudo systemctl reload ssh
```

防火墙只允许校园出口 IP 访问 TCP/22。确认第二个管理员会话可以正常登录前，不要关闭
当前会话，避免 SSH 配置错误导致机器失联。

## 5. 启动和检查

```bash
sudo systemctl enable --now ft-shadow-data-plane.service
sudo /opt/ft-shadow-data-plane/deploy/vultr/verify.sh
sudo journalctl -u ft-shadow-data-plane.service -f
```

`verify.sh` 会检查配置文件、不可变镜像地址、Compose 配置、SSH 语法、服务状态和数据
目录。它不会检查校园到 Vultr 的网络访问；该检查需要从校园 107 执行。

## 升级

检出新 release tag，重新运行 `install.sh`，只把 `EDGE_IMAGE` 更新为新的不可变 digest，
然后重启：

```bash
sudo systemctl restart ft-shadow-data-plane.service
sudo /opt/ft-shadow-data-plane/deploy/vultr/verify.sh
```

重启会产生明确记录的 planned gap。升级期间不得删除
`/srv/ft-data-sftp` 中尚未 ACK 的文件。
