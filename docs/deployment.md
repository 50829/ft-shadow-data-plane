# v0.3.2 端到端部署顺序

正式链路必须按以下顺序上线，避免 collector 产生数据后没有可用的异地持久化端。

1. 在 107 准备 `~/.ssh/ft-data-puller`，将公钥安全传给 Vultr 管理员；
2. 在 Vultr 安装 `rsync`、`rrsync`、Docker 和 OpenSSH，运行 `deploy/vultr/install.sh`；
3. Vultr 运行 `configure-rsync.sh` 安装 107 公钥，并独立核对 host-key 指纹；
4. 新装 107 时安装 v0.3.2 SIF；已有 v0.3.1 可保持不动，因为 v0.3.2 没有修改 central；
5. 107 配置 `central.yaml`，先用 `rsync --list-only` 和一次前台 pull 验证；
6. 107 安装每分钟 cron，确认至少两个周期均成功；
7. Vultr 写入 immutable image digest 和正式 60 币配置；
8. 启动 collector，运行 `verify.sh`，等待日志中的 `FORMAL_COLLECTION_STARTED`；
9. 确认 Vultr `ready/` 出现 chunk、107 `data/raw` 出现同一 chunk、Vultr 收到 ACK；
10. 连续观察 24 小时资源指标、gap、ACK 延迟和剩余磁盘。

v0.3.2 启动后必须确认日志没有持续的 `subscription audit`、`no mark_price event` 或 writer failure，
`control/collector-lease.json` 为当前 boot 的 `RUNNING`，且每分钟 ACK/ready 数量有进展。一次受控重启
应产生并在全源 ready 后关闭 `COLLECTOR_STOPPED_GAP`；不能通过删 gap 文件来获得质量通过。

详细命令分别见 [Vultr 手册](../deploy/vultr/README.md) 和
[107 手册](../deploy/campus-107/README.md)。

从 v0.3.0 升级 v0.3.1 不删除 raw，也不重置 Vultr generation/spool。尚未生成派生数据时，从
formal start 所在的首个 partial UTC day 开始按日期顺序提交；已有不可信的 v0.3.0 derived 可单独
删除后重算，不能删除对应 raw。

从 v0.3.1 升级 v0.3.2 只更新 Vultr edge image 和 `public_connection_shards: 4`。不得 clean
start，也不得重置 generation、formal-start、raw、ready、ACK、universe 或 collector lease。
受控重启产生的 stop gap 必须在所有 source 恢复后正常关闭。107 的 v0.3.1 pull 与 central
处理代码可继续使用；升级 107 仅用于统一版本标识，不是接收新 raw 的前置条件。

只有执行正式 clean start 时，才必须先停 collector 和 107 cron，解析并人工
核对每个绝对路径，再删除旧 `ready/writing/control` 与 107 的旧 `runtime/raw/derived`。
这些删除不可恢复；仓库 checkout 和 SSH 私钥不在删除范围内。
