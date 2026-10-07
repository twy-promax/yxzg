# tags/streaming — 实时类标签与实时数仓

> 包级说明。项目全局约定（运行环境、集群地址、通用陷阱）见项目根 `CLAUDE.md`；
> 基类流水线见 `tags/base/CLAUDE.md`；元数据 rule 格式见 `tags/bean/CLAUDE.md`。

## 职责

基于 **Kafka + Structured Streaming** 的实时链路：含实时数仓 ETL、指标计算，以及实时标签。

## 现状：7 个文件全部完成

| 文件 | 角色 | 状态 |
|---|---|---|
| `nginx_log_etl.py` | Nginx 日志 ETL | ✅ 继承 `StreamingETLBase` |
| `nginx_log_indicate.py` | Nginx 指标计算 → Doris | ✅ 继承 `StreamingIndicateBase` |
| `user_event_etl.py` | 用户行为日志 ETL | ✅ 继承 `StreamingETLBase` |
| `user_event_indicate.py` | 用户行为指标 → Doris + Kafka | ✅ 继承 `StreamingIndicateBase` |
| `shop_order_analysis.py` | 订单 12 小时窗口统计 → Doris + Kafka | ✅ 继承 `StreamingIndicateBase` |
| `conversion_rate.py` | **转化率标签**（rule_id=123） | ✅ 继承 `StreamingTagBase` |
| `active_tags.py` | **近期活跃度标签**（rule_id=127） | ✅ 继承 `StreamingTagBase` |

**7 个文件全部是基类子类**，每个只剩业务逻辑 + 一个 `__main__` 调用。
三个实时基类的接口说明见 `tags/base/CLAUDE.md`。

### ⚠️ 与文档规划的三点差异

1. **文件名对不上**：文档 5.7 写 `nginx_etl.py` / `nginx_analysis.py` / `user_event_analysis.py` / `conversion_tags.py`，
   实际是 `nginx_log_etl.py` / `nginx_log_indicate.py` / `user_event_indicate.py` / `conversion_rate.py`。**以代码为准**。
2. **三个实时基类的文件名/类名与规划不完全一致**：文档 9.6 规划的是
   `AbstractStreamingEtlBase` / `AbstractStreamingAnalysisBase` / `AbstractStreamingTagsBase`，实际是：

   | 文档规划 | 实际 |
   |---|---|
   | `streaming_etl_base.py` → `AbstractStreamingEtlBase` | `streaming_etl_base.py` → `StreamingETLBase` |
   | `streaming_analysis_base.py` → `AbstractStreamingAnalysisBase` | `streaming_indicate_base.py` → `StreamingIndicateBase`（**`analysis` → `indicate`**） |
   | `streaming_tags_base.py` → `AbstractStreamingTagsBase` | `streaming_tag_base.py` → `StreamingTagBase`（**少了个复数 `s`**） |
3. **实时标签类名不同**：文档规划 `ConversionTags` / `ActiveTags`，实际是 **`ConversionRate`** / **`ActiveTag`**（都少了复数 `s`）。

## 完整链路

```
log_generate/ 生成模拟日志 → source_data/ 文件
        │  Flume TAILDIR 采集（scripts/flume/*.conf）
        ▼
Kafka: xtzg_nginx_log / xtzg_user_event
        │
        ├── nginx_log_etl.py ──→ HDFS + Kafka: dwd_nginx_etl_result
        │        └── nginx_log_indicate.py ──→ Doris: log_analysis_db.nginx_log_result
        │
        └── user_event_etl.py ──→ HDFS + Kafka: dwd_user_event_etl_result
                 └── user_event_indicate.py ──→ Doris: log_analysis_db.user_event_result
                                            └→ Kafka: dws_user_event_analysis
                                                     │
                                        conversion_rate.py（转化率标签 → ES tags_id_streaming）

MySQL hive_data.shop_order
        │  SeaTunnel MySQL-CDC（initial 全量 + binlog 增量，产 Debezium 格式报文）
        ▼
Kafka: mysql_cdc.hive_data.shop_order
        │  shop_order_analysis.py（12 小时滑动窗口聚合）
        ▼
Kafka: dws_shop_order_analysis + Doris: db_analysis_db.shop_order_analysis
        │  active_tags.py（按 order_num 区间打标签）
        ▼
ES: tags_id_streaming（列 active_tag_time 记更新时间）
```

**Kafka 端口统一为 `up01:9092`**（两个 Kafka 类标签的 `nodes` 之前都误写成 9083，已修正）。

## 各文件的实现要点

> 7 个文件都只实现基类的抽象方法，读 Kafka、转类型、写出口等流程由基类接管。
> 下面记的是**各子类里剩下的业务逻辑**。

**`NginxLogETL.etl()`**（读 `xtzg_nginx_log`）：
- 正则用 `F.regexp_extract("value", pattern, N)` **按 group id 1~12 取值**，不是按命名组
- 时间：`dd/MMM/yyyy:HH:mm:ss Z` → `yyyy-MM-dd HH:mm:ss`
- **IP 转地区用高德 API**（`restapi.amap.com/v3/ip`，XML 输出），不是文档说的百度接口
- UA 解析用 `user_agents.parse`，`@F.udf` 返回 `MapType`，再按 key 取 `os`/`browser`/`device`
- 返回 `(etl_df, "dt")` → 基类写 HDFS（`/xtzg/etl/dwd_nginx_etl_result`）+ Kafka `dwd_nginx_etl_result`

**`NginxLogIndicate`**（读 `dwd_nginx_etl_result`）：
- `json_fields()` 返回 14 个字段名
- `indicate()` 按 `ip` 聚合出 9 个指标写 Doris；`uv` 直接用 `F.lit(1)`（已按 ip 分组，每组天然是一个独立访客）

**`UserEventETL.etl()`**（读 `xtzg_user_event`）：
- 嵌套 JSON 用 `F.get_json_object("value", "$.a.b")` 逐个取字段（**不能用 `json_tuple` 解析嵌套**）
- 返回 `(etl_df, "dt")` → 基类写 HDFS + Kafka `dwd_user_event_etl_result`

**`UserEventIndicate`**（读 `dwd_user_event_etl_result`）：
- `json_fields()` 返回 14 个字段名
- `indicate()` 按 `user_id` 聚合 13 个指标：
  - 流式下 `distinct` 聚合不可用，用 `F.approxCountDistinct` 代替（新版 API 是 `approx_count_distinct`，功能相同，旧写法会打 `FutureWarning`）
  - 停留时长：`(unix_timestamp(to_time) - unix_timestamp(browse_time))/60`

> ⚠️ 基类**没有 console 出口**，所以这 4 个任务跑起来控制台是安静的。要验证数据得查 Doris / Kafka，或者在子类里临时加一句 `result_df.writeStream.format("console").outputMode("update").start()`（非阻塞，能被基类后续出口接上）。

**`conversion_rate.py`** —— 转化率标签（rule_id=123）：
- **从元数据读 Kafka 配置**：`rule_obj.nodes`（broker）、`rule_obj.table`（topic）、`rule_obj.range`（startingOffsets）
- 算法：`buy_num / browse_num` → `ThreeTagIdJoiner.by_range(..., value_col="conversion_rate", user_col="user_id", keep_leftmost=True, tags_group="tags_id_streaming")`
- **已写入 ES**：只能用 `writeStream.foreachBatch` + `format("es")`，**没有** `writeStream.format("es")` 这种写法。
  写 ES 用 `es.mapping.id=user_id` + `es.write.operation=upsert`，落 ES 的 **`tags_id_streaming`** 字段
- 同时输出一份到 console 便于观察；`outputMode("update")`（流式聚合不能用 `append`）

**`shop_order_analysis.py`** —— 订单 12 小时窗口统计（读 `mysql_cdc.hive_data.shop_order`）：
- ⚠️ **重写的是 `json_parse`，不是 `json_fields`**：Debezium 报文是 `before`/`after` 两层嵌套，`json_tuple` 解析不了，
  必须用 `get_json_object` 逐字段取，所以 `json_fields()` 返回空元组
- 取值规则：`op == 'd'`（删除）取 `before`，其余取 `after`；三个金额字段一律取差值 `after - before`，null 当 0
- 订单号：`parent_order_no` 为空或空串时取 `order_id`
- `indicate()`：`withWatermark('create_time','10 minutes')` +
  `groupBy('user_id', F.window('create_time','12 hours','1 minutes').cast(StringType()))`，
  用 `approx_count_distinct('order_sn')` 统计去重下单量
- ⚠️ **重写了 `create_SparkSession`** 关掉 whole-stage codegen（原因见下面「codegen 编译失败」）
- 输出：Doris `db_analysis_db.shop_order_analysis` + Kafka `dws_shop_order_analysis`

**`active_tags.py`** —— 近期活跃度标签（rule_id=127，读 `dws_shop_order_analysis`）：
- 本包**最短的实现**：上游的 `order_num` 已经是"该用户 12 小时内的下单量"，
  所以不用再做窗口聚合，直接 `by_range(..., value_col="order_num", user_col="user_id", tags_group="tags_id_streaming")`
- ⚠️ **不需要 `keep_leftmost`**：5 级区间 `4-1000` / `2-3` / `0-1` **互不共享端点**
  （这点和转化率不同 —— 那边 `0.6~1` 与 `1~3` 共享端点 1，必须去重）

## ⚠️ 本包最密集的坑（文档 5.7 原文标注，实测有效）

**正则与 JSON 解析**
- `F.regexp_extract` 的 **Java 正则只能用 group id 取值**，命名组写作 `(?<name>...)`，**别名不能带下划线**。
- `F.get_json_object` **每次只能取一个 key**，但**支持任意层级嵌套**；`F.json_tuple` 一次能取多个，但**不支持嵌套 json**。选型看数据结构。
- **同一个 select 中不能多次使用 `json_tuple`**。

**流式聚合限制**
- 流式 DataFrame **不支持 `distinct` 聚合**，必须改用 `approx_count_distinct()` / `approxCountDistinct()`。
- **codegen 编译失败**（`ShopOrderAnalysis` 踩到过，已修复）：该任务的 `json_parse` 里有 8 个表达式、
  每个又套多个 `when/otherwise`，Spark 把整条查询编译成的 Java 方法**超过 JVM 单方法 64KB 上限**，Janino 报
  `CompileException: File 'generated.java', ... A method named "expand_switchCaseCode_xxx" is not declared in any enclosing class`。
  修法：在 `ShopOrderAnalysis.create_SparkSession` 里重写并 `spark.conf.set("spark.sql.codegen.wholeStage","false")`
  —— **只关该任务**，其他任务保留 codegen 加速。
  **识别特征**：报错里出现 `generated.java`，说明是 Spark 动态编译的代码出问题，**不是 Python 语法错**。
  替代方案：调大 `spark.sql.codegen.methodSplitThreshold`（默认 1024），把超长方法拆小，可保留 codegen。

**UDF 相关**
- IP 解析、UA 解析都是 `@F.udf`；UA 解析若需传额外参数要用**闭包返回 UDF**（UDF 不能直接传非列名参数）。
- 文档提到 `NginxAnalysis` 的 `ip_to_id` 要把 IP 各段补零再 `cast(LongType())`（因为 Doris 主键不能是 string）—— 当前**没有实现这个**，`nginx_log_result` 的主键直接用了 `ip varchar(15)`（见 `scripts/doris_scripts/doris_create_table_sql.sql`）。

**HDFS 写出**
- `partitionBy('dt')`、`format('orc')`、`trigger(processingTime='5 seconds')`
- 建 Hive 外部表后需 `MSCK REPAIR TABLE` —— ⚠️ **目前 `scripts/hive_scripts/hive_create_table.sql` 里只有 dwd/dwm 那 5 张业务表，没有建这两个日志结果的外部表**

**Debezium 报文（`shop_order_analysis.py`，已落地）**
- 文档原建议 `from_json` + 自定义 `StructType`；**实际用的是 `get_json_object` 逐字段取**，两者都可行
- 取值：`op == 'd'` 取 `before`，否则取 `after`；金额字段取差值 `after - before`
- 订单号：`parent_order_no` 为空或空串时取 `order_id`
- 窗口：`withWatermark('create_time', '10 minutes')` +
  `groupBy('user_id', F.window('create_time', '12 hours', '1 minutes').cast(StringType()))`
  —— 窗口**必须 `cast(StringType())`**，struct 类型不能直接作为分组列输出

## Streaming 写出约定

- 有聚合时输出模式**统一用 `update`**（`append` 不能聚合、`complete` 每次输出全量）
- 无原生 Doris sink，**统一用 `ForeachBatch sink`** 走 JDBC（端口 **9030**），`mode="append"`
- 默认 at-least-once；因 Doris Unique 模型幂等，**不需要精确一次**
- checkpoint 落在 `hdfs://up01:8020/streaming_chk/{appName}_{sink}`（如 `conversion_rate_es`、`nginx_log_etl_hdfs`），
  由各基类用 `writeStream.option("checkpointLocation", ...)` **显式指定**。
  ⚠️ 不能只用 SparkSession 的 `spark.sql.streaming.checkpointLocation` 全局配置 —— 那样 Spark 会再拼一个随机 UUID，
  路径每次启动都不同，等于每次都从头消费。详见 `tags/base/CLAUDE.md`
- 要输出到多个地方时，只能在**最后一个 `start()`** 后面跟 `awaitTermination()`

## 实时标签的特殊约定

- 写 ES 时**只能通过 `writeStream.foreachBatch`** —— 没有 `writeStream.format("es")` 这种集成方式
- **写入字段是 `tags_id_streaming`**（区别于离线标签的 `tags_id_times`、SQL 系标签的 `tags_id_once`）。
  `ThreeTagIdJoiner.by_range` / `by_equal` 为此新增了 `tags_group` 参数（默认 `"tags_id_times"`），
  实时标签调用时传 `tags_group="tags_id_streaming"`
- ⚠️ **流式下不能用 `keep_leftmost` 的批式实现**：它依赖 `row_number()`，而流式 DataFrame
  **不支持非时间窗口**，会报 `Non-time-based windows are not supported on streaming DataFrames`。
  `by_range` 已按 `business_df.isStreaming` 自动分流 —— 流式改用 `groupBy` + `min(struct(start, id))`
  （struct 按字段顺序比较，取到的就是最左侧那条），**但这要求 `outputMode("update")`**
- 文档要求"同时写入标签更新时间列 `{app_name}_time`"—— 已由 **`StreamingTagBase.add_update_time()`** 统一实现，
  在 `execute` 里自动调用，列名取自 `appName`（`conversion_rate` → `conversion_rate_time`，`active_tag` → `active_tag_time`）。

> ⚠️ **ES 索引里会多出这两个 `*_time` 字段，属于预期行为**。
> `user_profile_tags` 的 mapping 里**没有**定义它们，但索引也没设 `dynamic`（ES 默认 `true`），
> 所以 ES 在建索引时**自动创建**了这两个字段，类型被推断成 **`text`**
> —— 因为 `"2026-09-26 23:33:50"` 不符合 ES 默认的 `strict_date_optional_time` 格式（要求带 `T`）。
>
> **当前定位：仅供前端展示。** 存读都正常，但**不能按时间范围查询 / 排序**（text 走字符串字典序，
> 结果不正确，在 Kibana 里也识别不成时间轴）。
> 若将来要按它筛数据，得把类型改成 `date` —— **ES 不允许修改已有字段类型，必须 reindex 重建索引**，
> 所以趁数据量小的时候改最划算。
>
> 另：需求规定每个实时标签各占一个 `*_time` 列，字段会随标签数量增长。
> 如果只是想要"记录最后更新时间"这种统一语义，共用一个 `update_time` 更省 —— 但需求点名了 `{app_name}_time`，按需求来。

## 元数据现状

| 标签 | rule_id | `nodes`（Kafka） | topic（`table`） | 状态 |
|---|---|---|---|---|
| 转化率 | 123 | `up01:9092` ✅ | `dws_user_event_analysis` | ✅ 已上线（写入 ES `tags_id_streaming`） |
| 近期活跃度 | 127 | `up01:9092` ✅ | `dws_shop_order_analysis` | ✅ 已上线（写入 ES `tags_id_streaming`） |

> 两个实时标签的 `range` 都是 `earliest` —— 这是**测试期配置**（便于反复重跑全量）。生产环境会去掉它走默认的 `latest`，
> 但**前提是 checkpoint 落点已固定**，否则会从"重复处理"变成"静默漏数据"。详见 `tags/base/CLAUDE.md`。

> 元数据里 `inType=Kafka` 的标签，其 `nodes` 是 Kafka broker 地址、`table` 是 topic、`range` 是 `startingOffsets` ——
> 与离线标签（`nodes`=Hive Metastore、`table`=库.表、`range`=天数）的含义**完全不同**，别混淆。
