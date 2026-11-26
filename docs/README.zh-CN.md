# CDC EX

[English](../README.md) · [运行手册](RUNBOOK.md) · [发布候选版](https://github.com/xzcode/cdc-ex/releases)

CDC EX 是一个自托管的单向数据库变更捕获与同步工具，包含原生 CDC 连接器、可选 Kafka 输入输出、持久化事件日志、显式字段映射，以及英文/简体中文管理界面。

**当前版本：0.1.0-rc.1，M1–M9 功能已实现。** 发布流程要求完整 [V1 CI](https://github.com/xzcode/cdc-ex/actions/workflows/ci.yml) 对同一提交、同一份 JAR 全部通过。具体版本和链路以[验收矩阵](ACCEPTANCE.md)、发布附件中的构建信息与测试报告为准；连接器存在不表示所有数据库版本或任意两端组合都已验证。

## 支持范围

| 连接器 | 作为源端 | 作为目标端 |
| --- | --- | --- |
| MySQL | 初始快照 + ROW/FULL binlog | 按主键事务写入、更新、删除 |
| MariaDB | 独立 MariaDB 捕获器，快照 + binlog | 按主键事务写入、更新、删除 |
| MongoDB | 副本集一致边界快照 + 精确前后镜像 Change Streams | 保留 BSON 类型的文档替换、插入、删除 |
| PostgreSQL | 初始快照 + pgoutput 逻辑 WAL | 按主键事务写入、更新、删除 |
| ClickHouse | 有限快照、受约束追加游标、应用维护的变更表 | 事件历史或版本化当前状态 |
| Kafka | 按分区消费保留的 CDC EX 事件 | 按键发布 CDC EX 事件 |

Kafka 是可选端点，数据库直连同步不依赖 Kafka。MongoDB 当前要求副本集，拒绝分片集群。ClickHouse 普通表没有通用原生行级 CDC；追加游标不能推断任意更新、删除或落在已读水位之前的迟到记录。

## 启动

在仓库根目录安装 Docker Compose 后，首次生成并妥善保存部署密钥：

```bash
umask 077
if [ ! -e .env ]; then
  printf 'CDC_ADMIN_TOKEN=%s\nCDC_MASTER_KEY=%s\n' \
    "$(openssl rand -hex 32)" "$(openssl rand -base64 32)" > .env
fi
docker compose up -d --build
```

访问 **http://127.0.0.1:8080**，输入 `.env` 中的管理员令牌。创建源连接和目标连接，按实际角色测试，创建任务并验证映射后启动。连接里的数据库地址必须从容器内部可达；容器里的 `127.0.0.1` 指向 CDC 容器本身。

JAR 模式需要 Java 21。设置 `CDC_MODE=server`、`CDC_DATA_DIR`、`CDC_ADMIN_TOKEN`，并设置 `CDC_MASTER_KEY` 或受保护的 `CDC_MASTER_KEY_FILE`，然后执行 `java -jar cdc-ex-0.1.0-rc.1.jar`。完整配置、API 示例、权限要求和恢复操作见[运行手册](RUNBOOK.md)。

从源代码构建需要 JDK 21、Maven 3.9、Node 24：

```bash
npm --prefix cdc-web ci
npm --prefix cdc-web run build
mvn -B -ntp clean verify
```

## 可靠性与边界

源端位点在事件整批落入 SQLite 后才推进，目标端确认写入后再推进独立消费水位。崩溃边界可能产生重复投递，所以语义是 **at-least-once**。稳定主键状态写入和持久化 ClickHouse 版本支持重放；Kafka 和事件历史仍可能出现重复事件。不承诺跨异构系统 exactly-once，也不保持源事务在任意目标上的整体原子性。

每个任务包含一个表、集合或主题，单节点最多四个活跃任务。DDL 仅提供预览，不自动执行破坏性变更。目标和源的身份、结构变化，以及过期位点、无效事件、容量耗尽都会显式阻断。数据对比需要业务写入静止，并受行数与字节上限约束。

已投递负载会压缩成防重放回执。默认 256 MiB 逻辑容量、100 万个任务生命周期事件身份；回执不会因时间自动丢弃。需要时可在任务停止后显式提高限制，或按手册建立已核对的新任务代次。不要删除位点文件绕过恢复检查。

## 文档

[范围与限制](V1_SCOPE.md) · [架构](ARCHITECTURE.md) · [连接器接口](CONNECTORS.md) · [映射](MAPPINGS.md) · [安全](SECURITY.md) · [阶段交付](ROADMAP.md) · [验收](ACCEPTANCE.md) · [发布说明](RELEASE_NOTES.md)

发布附件包含经过验收的 JAR、对应源码、SHA-256 校验、CycloneDX SBOM、实际打包依赖清单、构建来源以及实测工作负载报告。项目自身许可证尚未确定，第三方组件遵循各自许可证。
