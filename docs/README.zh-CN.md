# CDC EX

[English](../README.md)

轻量、自托管的数据库变更捕获与实时同步平台。自主开发任务管理、持久化投递、数据映射及管理界面，复用成熟的变更捕获底层库。

当前状态：M0 设计基线，尚未实现连接器。

首版仅支持 MySQL、MariaDB、MongoDB、PostgreSQL、ClickHouse。前四种作为原生 CDC 源端及写入目标；ClickHouse 作为目标端优先，同时提供全量读取、受约束的增量游标或显式变更表读取，不将普通查询轮询冒充完整 CDC。

技术栈：Java 21、Maven、Spring Boot、Debezium Engine、SQLite、Vue 3、TypeScript。首版单节点，不要求 Kafka 或 Flink。

开发规范：[首版范围](V1_SCOPE.md) · [架构](ARCHITECTURE.md) · [连接器规范](CONNECTORS.md) · [开发路线](ROADMAP.md)。
