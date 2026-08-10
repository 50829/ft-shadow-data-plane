# 校园 107 拉取和处理

这个目录是校园 107 机器唯一需要阅读的部署入口。login node 每分钟执行一次短时 SFTP
拉取；消耗 CPU 和内存的规范化及 L2 重建只能作为 Slurm job 运行。

安装前必须获得校园管理员对 login-node cron 的正式许可。

## 1. 前置条件

- Apptainer、Slurm 客户端命令、`flock` 和 OpenSSH 客户端；
- 挂载在 `/persistent/ft-shadow-data-plane` 的持久存储；
- Release 产物 `ft-shadow-data-plane.sif` 和
  `ft-shadow-data-plane.sif.sha256`；
- 已被 Vultr 的 `data-puller` 账户授权的私钥；
- 通过独立渠道核对过的 Vultr SSH host key 指纹。

## 2. 校验并安装 release

在下载目录校验产物：

```bash
sha256sum --check ft-shadow-data-plane.sif.sha256
```

然后使用生产账户，在仓库 checkout 根目录执行：

```bash
./deploy/campus-107/install.sh /path/to/ft-shadow-data-plane.sif
```

安装器会把基于 hash 命名的不可变 SIF 和校园端脚本安装到
`/persistent/ft-shadow-data-plane`，创建 `raw`、`derived` 和 `symbols` 目录，
并且只在配置不存在时创建配置。它不会安装 cron，也不会连接 Vultr。

## 3. 配置拉取和处理

编辑：

```text
/persistent/ft-shadow-data-plane/central.yaml
/persistent/ft-shadow-data-plane/deploy/campus-107/processing.env
```

`central.yaml` 配置 Vultr 地址、私钥、固定的 `known_hosts` 和本地 raw 路径。
`processing.env` 配置 SIF、raw/derived 路径和 collector ID。所有路径都必须位于 login
node 和 Slurm worker 共同可见的存储上。

只有通过独立渠道比对 Vultr host key 指纹后，才能写入 `known_hosts`。禁止使用
`AutoAddPolicy` 或关闭 host-key 检查。

先执行本地检查和一次前台拉取：

```bash
/persistent/ft-shadow-data-plane/deploy/campus-107/verify.sh
apptainer exec /persistent/ft-shadow-data-plane/ft-shadow-data-plane.sif \
  ft-data-pull --config /persistent/ft-shadow-data-plane/central.yaml
```

安装 cron 前，确认 raw chunk 已出现在 `/persistent/ft-shadow-data-plane/raw`，并且
Vultr 上出现对应 ACK。

## 4. 安装拉取 cron

运行 `crontab -e`，添加以下文件中非注释的任务：

```text
/persistent/ft-shadow-data-plane/deploy/campus-107/crontab.example
```

puller 不能作为 login node 常驻服务运行。`flock` 用来防止相邻两分钟的任务重叠。

## 5. 处理一个 sealed UTC 日

创建 symbol 文件，每行只能有一个不重复的大写 Binance symbol，不能有空行：

```text
BTCUSDT
ETHUSDT
```

一次提交 normalize、受并发限制的 L2 array 和 finalize：

```bash
/persistent/ft-shadow-data-plane/deploy/campus-107/submit-day.sh \
  2026-08-10 /persistent/ft-shadow-data-plane/symbols/2026-08-10.txt
```

脚本会根据 symbol 文件自动计算 Slurm array 范围并打印三个 job ID。只有
`SEALED.json` 和它引用的全部 chunk 都已下载并通过 hash 校验后，才能提交当天任务。

## 升级

校验新 SIF，使用新文件重新运行 `install.sh`，再运行 `verify.sh` 和一次前台 pull。
安装器会保留现有 YAML 和环境配置，旧的 hash-named SIF 也会保留，避免影响正在运行的
Slurm job。
