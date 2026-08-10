# 部署入口

同一个版本会部署到两类机器。先选择目标机器；对应目录包含该机器所需的全部配置、服务文件
和操作说明。

| 目标 | 职责 | 从这里开始 |
| --- | --- | --- |
| Vultr 边缘机 | 采集 Binance 数据，通过受限 SFTP 发布 sealed chunk | [`vultr/README.md`](vultr/README.md) |
| 校园 `107` | 拉取 raw，向 Slurm 提交规范化和 L2 任务 | [`campus-107/README.md`](campus-107/README.md) |

部署时检出固定 release tag，然后只阅读目标机器的 README。运行配置和凭据只保留在对应
机器上，不得提交到仓库。

Release 页面提供同一份代码的两种产物：

- Vultr Docker 部署使用的不可变 OCI image digest；
- 校园 Apptainer 部署使用的 `ft-shadow-data-plane.sif` 及其 SHA-256 文件。
