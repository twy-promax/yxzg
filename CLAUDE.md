# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目定位

电商（"云鲜智购"）**用户画像标签体系 + 推荐系统**大数据工程。数据链路：

    MySQL(hive_data) --SeaTunnel--> Hive(dwd/dwm) --PySpark--> Elasticsearch(user_profile_tags)
                                              ^
                          标签元数据 MySQL(tags_info.tbl_basic_tag)

纯 Python/PySpark 工程，无 Scala/Java 源码（Scala 只出现在 jar 文件名里）。
需求全文见 `docs/云鲜智购用户画像及推荐系统-项目需求.md`（14 章），设计细节以该文档为准。

## 运行环境（硬性约束：代码只在 Linux 集群运行）

| 项 | 值 |
|---|---|
| 集群主机名 | `up01`（= 192.168.88.166） |
| Spark | 3.3.2（bin-hadoop3，Scala 2.12） |
| Hive | 3.1.2，Metastore `thrift://up01:9083`，HS2 `up01:10000`，Thrift Server `up01:10001` |
| HDFS | `hdfs://up01:8020`（NameNode Web UI `up01:9870`） |
| Elasticsearch | 7.10.2，`up01:9200` |
| MySQL | 5.7.29，`up01:3306`，`root/123456` |
| Doris | 1.2.1，FE `up01:9030` |
| Python | 3.10.9（Anaconda） |
| SeaTunnel | 2.3.5，位于 `/export/server/apache-seatunnel-2.3.5` |
| DolphinScheduler | 3.1.2，`http://up01:12345/dolphinscheduler/ui` |
| Linux 侧代码目录 | `/export/data/workspace/user_profile` |

Windows 开发机上**跑不了任何代码**：`up01` 不可达，代码里硬编码了 Linux 路径（如 `tags/base/tags_base.py:13-15`）。
验证只能在 Linux 侧做，`.idea/deployment.xml` 配了 AutoUpload 自动同步到远端目录。

**生成代码时的约束：**
- 路径一律 Linux 风格（`/export/...`），不出现盘符或反斜杠分隔符
- 文件编码 UTF-8，行尾 LF
- 不引入平台判断 API（`os.name` / `platform.system` / `win32`）
- **`tags/utils/` 下的工具类默认采用「面向对象 + 静态方法」**：用一个类承载一组相关操作，方法**优先声明为 `@staticmethod`**，调用时直接用类名（如 `ThreeTagIdMapper.to_udf(three_tag_df)`），**不要实例化**；只有静态方法无法满足（需要保存实例状态）时，才退而使用实例方法。

## 包级文档索引

`tags/` 下每个包都有独立 CLAUDE.md，**改动某个包前先读它**：

| 包 | 文档 | 一句话职责 |
|---|---|---|
| `tags/base/` | [CLAUDE.md](tags/base/CLAUDE.md) | 4 个基类：离线 `TagBase`（10 步流水线、标签合并）+ 实时 `StreamingETLBase` / `StreamingIndicateBase` / `StreamingTagBase` |
| `tags/bean/` | [CLAUDE.md](tags/bean/CLAUDE.md) | 数据实体类；**标签元数据 rule 格式与 level 层级在此** |
| `tags/match/` | [CLAUDE.md](tags/match/CLAUDE.md) | 匹配类标签（6 个全部实现，含各标签开发要求） |
| `tags/statistics/` | [CLAUDE.md](tags/statistics/CLAUDE.md) | 统计类标签；**`where_condition` 时间过滤与 `range` 现状在此** |
| `tags/ml/` | [CLAUDE.md](tags/ml/CLAUDE.md) | 挖掘类标签（K-Means；USG 的决策树版已放弃） |
| `tags/streaming/` | [CLAUDE.md](tags/streaming/CLAUDE.md) | 实时类标签与实时数仓 |
| `tags/recommend/` | [CLAUDE.md](tags/recommend/CLAUDE.md) | 推荐系统：流行度 + 关联规则 + Flask 接口服务（其余算法线已放弃） |

`tags/utils/` 无独立文档（工具类，现有 `RuleParse`、`ThreeTagIdMapper`、`ThreeTagIdJoiner`、`HDFSUtil`）。

## 当前进度（重要）

项目处于**开发后期**：匹配类、统计类、实时类标签均已全部完成，挖掘类已起步（客户价值、PSM 已完成），推荐系统已部分完成（流行度 + 关联规则 + Flask 接口），组合标签尚未开始：

| 范围 | 状态 | 位置 |
|---|---|---|
| `TagBase` 框架（含 `where_condition` 时间过滤、`default_tag` 全量用户兜底） + `RuleParse` 解析 | ✅ 已完成 | `tags/base/`、`tags/utils/` |
| 匹配类标签（性别、年龄、职业、政治面貌、婚姻状况、国籍） | ✅ 已完成 | `tags/match/` |
| 统计类标签（元数据里 8 个**全部已实现**；文档另规划的 5 个比率类标签元数据未建档） | ✅ 已完成 | `tags/statistics/` |
| 挖掘类标签 | 客户价值(39) ✅、PSM(52) ✅；**与统计类 DSL 路线并存**（同标签两条路线会互相覆盖，见 `tags/ml/CLAUDE.md`）；USG(58) 的挖掘版 ❌ **已放弃**（统计类 `usg_tag.py` 仍在） | `tags/ml/` |
| 实时类标签（7 个文件全部完成：4 个实时数仓组件 + 转化率 / 近期活跃度两个标签） | ✅ 已完成 | `tags/streaming/` |
| 组合标签 | ❌ 未开始 | `tags/bean/` 或待定（见下） |
| 推荐系统 | 部分完成：近期热门（流行度）+ FP-Growth 关联规则 + Flask 接口服务 ✅；个人热门 / ALS / ItemCF / UserCF **已放弃** | `tags/recommend/` |
| Hive 建表 DDL / SeaTunnel 同步 / ES 索引脚本 | ✅ 已就绪 | `scripts/` |

**不要假设未开发的模块已有实现**。开发依据全在 `docs/云鲜智购用户画像及推荐系统-项目需求.md`：5.4–5.8 节分别对应匹配 / 统计 / 挖掘 / 实时 / 组合标签需求，第 6 章是推荐系统，7 章是调度部署。

> 附注：**需求文档 9.5/9.6 节规划的工程结构与实际代码差异较大**（文件名、类名、基类数量都对不上），各包 CLAUDE.md 里已逐条记录差异。以实际代码为准，但要迁就文档规划时先看差异记录。

## 目录角色（非 tags 部分）

- `scripts/hive_scripts/` 建库建表 DDL —— `hive_create_table.sql` 建 dwd/dwm 业务表，`hive_ads_create_table.sql` 建 `ads` 库的 3 张标签结果表（RFM / 新老会员 / 合并表）
- `scripts/seatunnel_scripts/` SeaTunnel 任务配置 —— `loadData.config` 等把 MySQL 同步到 Hive；`hive2es.config` 把合并后的 `ads.ads_mem_tags_i` 指定分区送进 ES（标签落 ES 的 `tags_id_once` 字段）；`test_mysql_cdc_config` 用 MySQL-CDC 把 `hive_data.shop_order` 的 binlog 变更以 Debezium 格式送到 Kafka（供 `shop_order_analysis.py` 消费）
- `scripts/flume/` Flume 采集配置 —— `nginx_to_kafka.conf`、`user_event_to_kafka.conf` 用 TAILDIR 监听日志文件，经 KafkaChannel 发到对应 topic
- `scripts/doris_scripts/` Doris 建表 SQL —— `log_analysis_db` 的两张实时指标结果表（`nginx_log_result`、`user_event_result`）+ `db_analysis_db.shop_order_analysis`（订单 12 小时窗口统计），均为 UNIQUE 模型；另有 `finebi_dashboard_views.sql` —— **FineBI 大屏专用**：用户标签长表 `recommend_db.user_tag_long` + KPI 表 `user_tag_stat` + 30 个视图（A 屏 18 个 / B 屏 12 个），脚本可重复执行，每次标签跑完批重跑第二节两条刷新语句（`TRUNCATE` + `INSERT INTO`）即可。⚠️ **不能用 `INSERT OVERWRITE`**：Doris 1.2.1 的它只支持分区表，而这两张是非分区表，实测直接报语法错误
- `scripts/finebi_a_user_profile/` **大屏 A（用户画像）的 FineBI 资产** —— `操作清单.md`（逐步建屏步骤，首选）+ README + 21 个 SQL 文件（备选：17 个标签分布图 + 4 个 KPI，**一个文件 = 一个 FineBI SQL 数据集 = 一个组件**）。SQL 版依赖 Doris 的 ES catalog（读 `user_profile_tags`）与 MySQL catalog（读 `tbl_basic_tag`），⚠️ **用前必须替换 SQL 里的 catalog 表名**（截图里实际是 `es` / `jdbc_mysql`）。3 级标签 id 全局唯一，按 `pid` 过滤即可把各标签类别分开
- `scripts/finebi_b_realtime/` **大屏 B（实时运营）的 FineBI 资产** —— `操作清单.md`（逐步建屏 + 刷新频率，首选）+ README + 12 个 SQL 文件（备选，全部查 Doris 内部表，可直接用）。⚠️ 三张实时表都是 UNIQUE 快照（每实体一行、新数据覆盖旧数据），**不能做时间趋势**，`shop_order_analysis` 也不能做加总
- `scripts/*.edql` ES 索引与查询（IntelliJ EDQL 插件脚本，不是编程语言）
- `log_generate/` **日志模拟生成工具**（第三方引入）：生成 Nginx 访问日志与用户行为日志到 `datacollection/source_data/`，供 Flume 采集；自带 Kafka Producer/Consumer 工具类。注意 `resource/config.ini` 里的 topic 名是 `hmzx_*`，与实际使用的 `xtzg_*` 不一致
- `test/` 零散验证脚本（ES 连通性、UA 解析、IP 解析等），**不是单元测试**，无测试框架
- `jar包/` 手工下载的第三方 jar（ES-Hadoop、Kafka、Hive-JDBC、SeaTunnel 连接器），**须手动放到 Linux 侧对应目录才生效**
- `data/` 本地参考资料：`data/mysql/*.sql` 是 MySQL 导出的真实数据（`tags_info.sql` 标签元数据、`hive_data.sql` 业务数据、`update_data.sql` 日期平移脚本）；`iris.csv`、`sample_movielens_ratings.txt`、`medium.txt` 是与本项目无关的练习数据
- `docs/` 开发依据 —— `云鲜智购用户画像及推荐系统-项目需求.md`（需求全文，14 章）+ 两张规划表：`云鲜智购-标签体系.csv`（72 条规划标签，**只有标签名、没有 id 列**）、`云鲜智购-组合标签.csv`（3 条）。⚠️ **规划 ≠ 元数据已建档**：`tbl_basic_tag` 里没有记录的标签做不了（如统计类的 5 个比率标签）。两个 CSV 带 UTF-8 BOM，本地读取用 `utf-8-sig`

## 常用命令

无构建 / 测试 / lint 体系，无 `requirements.txt`。实际运行方式：

```bash
# 跑单个标签（tags 目录须先打成 tags.zip）
spark-submit --master yarn --deploy-mode client --py-files tags.zip tags/match/age_tag.py

# SeaTunnel 数据同步
/export/server/apache-seatunnel-2.3.5/bin/seatunnel.sh \
  --config scripts/seatunnel_scripts/loadData.config -e local
```

生产调度走 DolphinScheduler：Spark 任务用 SPARK 节点提交 `--py-files tags.zip`；SQL 任务用 SQL 节点（Hive 数据源 `up01:10000`，库 `dwm`）。

`tags/` 下**所有子目录都必须是 Python Package**（含 `__init__.py`），否则 `--py-files tags.zip` 后 import 会失败。

## 已知陷阱（跨包通用）

- **ES 索引必须先手动创建**：`user_profile_tags` 若交给 Spark 自动建，`tags_id_times` 会被推断成 `long`，后续写字符串报类型不匹配。建索引脚本见 `scripts/user_profile_tags.edql`（含 `comma` 分词器定义）。
- **ES-Hadoop jar 须放入 Spark 的 `jars/` 目录**才能用 `format("es")`。当前 `jar包/elasticsearch-spark-30_2.12-8.11.4.jar`（对应 Spark 3.0 / ES 8.x）与实际环境 Spark 3.3.2 / ES 7.10.2 存在版本错配，上集群须先验证。
- **`master("local[*]")` 硬编码**在 `tags/base/tags_base.py:28`，DolphinScheduler 选 cluster 模式会失败。
- **Java 正则语义**：用 `F.regexp_extract` 时命名组不能带下划线（底层是 Java 正则）。详见 `tags/streaming/CLAUDE.md`。
- **文档与代码命名不一致**：需求文档写 `AbstractTagsBase` / `AgeTags` / `gender_tags.py`，实际是 `TagBase` / `AgeTag` / `gender_tag.py`（类名与 match 下的文件名都少了复数 `s`）。基类文件名 `tags/base/tags_base.py` 现已与文档对齐。**以代码为准**。

## 仓库状态

`git log` 为空 —— 仓库尚无任何提交，所有文件未跟踪，改动前没有回滚能力。

`data/mysql/hive_data.sql` 有 **124MB**，直接提交会让仓库体积迅速膨胀。纳入版本控制前需先决定处理方式（`.gitignore` 排除，或改用其他方式共享）。
