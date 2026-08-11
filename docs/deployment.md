# v0.2 端到端部署顺序

正式链路必须按以下顺序上线，避免 collector 产生数据后没有可用的异地持久化端。

1. 在 107 准备 `~/.ssh/ft-data-puller`，将公钥安全传给 Vultr 管理员；
2. 在 Vultr 安装 `rsync`、`rrsync`、Docker 和 OpenSSH，运行 `deploy/vultr/install.sh`；
3. Vultr 运行 `configure-rsync.sh` 安装 107 公钥，并独立核对 host-key 指纹；
4. 在 107 安装 v0.2 SIF，安装器构建 hash-named writable sandbox；
5. 107 配置 `central.yaml`，先用 `rsync --list-only` 和一次前台 pull 验证；
6. 107 安装每分钟 cron，确认至少两个周期均成功；
7. Vultr 写入 immutable image digest 和正式 60 币配置；
8. 启动 collector，运行 `verify.sh`，等待日志中的 `FORMAL_COLLECTION_STARTED`；
9. 确认 Vultr `ready/` 出现 chunk、107 `data/raw` 出现同一 chunk、Vultr 收到 ACK；
10. 连续观察 24 小时资源指标、gap、ACK 延迟和剩余磁盘。

详细命令分别见 [Vultr 手册](../deploy/vultr/README.md) 和
[107 手册](../deploy/campus-107/README.md)。

本版本没有旧状态迁移。若执行正式 clean start，必须先停 collector 和 107 cron，解析并人工
核对每个绝对路径，再删除旧 `ready/writing/control` 与 107 的旧 `runtime/raw/derived`。
这些删除不可恢复；仓库 checkout 和 SSH 私钥不在删除范围内。
