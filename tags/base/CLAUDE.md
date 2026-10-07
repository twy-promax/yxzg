# tags/base — 基类

> 包级说明。项目全局约定（运行环境、集群地址、通用陷阱）见项目根 `CLAUDE.md`。

## 职责

存放标签工程的抽象基类。其他包下的标签类继承这里的基类，只实现抽象方法。

## 现状

存放 **4 个基类**：

| 文件 | 类名 | 支撑的子类 |
|---|---|---|
| `tags_base.py` | `TagBase` | 离线标签：`tags/match/` 6 个 + `tags/statistics/` 6 个 DSL 系 + `tags/ml/` 客户价值 1 个 |
| `streaming_etl_base.py` | `StreamingETLBase` | 实时 ETL：`NginxLogETL`、`UserEventETL` |
| `streaming_indicate_base.py` | `StreamingIndicateBase` | 实时指标：`NginxLogIndicate`、`UserEventIndicate` |
| `streaming_tag_base.py` | `StreamingTagBase` | 实时标签：`ConversionRate` |

`TagBase` 目前支撑 **13 个标签子类**（match 6 + statistics 6 + ml 1）。

但它**仍不是**文档规划中那个能承载全部离线标签的通用基类 —— 挖掘类需要的模型复用能力**没有做进基类**，而是由 `tags/ml/value_tag.py` 在自己的 `compute` 里调 `HDFSUtil.isexists` 自行实现。

"全量用户"语义**已补齐**：`merge_tag` / `execute` 增加了 `default_tag` 参数，需要全量语义的标签（消费周期、客户价值）传一个默认 3 级标签 id，本轮没算到的用户就会被补上该默认值。详见下面「标签合并机制」。

## `TagBase.execute()` 流水线

`execute(two_tag_id, app_name, where_condition=None, default_tag=None)` 是模板方法，固定 10 步，子类不重写：

1. `init_spark` 建 SparkSession（`enableHiveSupport` + Hive Metastore）
2. `get_mysql` 读全部标签元数据（`tags_info.tbl_basic_tag`）
3. `parse_tag_rule` 取 2 级标签 `rule`，交给 `RuleParse.parse()`
4. `read_hive` 按解析出的 `table` / `selectFields` 查 Hive 业务数据，可拼入 `where_condition` 做时间过滤
5. `read_three_tag` 取该 2 级标签下所有 3 级标签（`id`, `rule`）
6. **`compute`（唯一抽象方法，子类必须实现）** —— 打标签
7. `read_es` 读 ES 旧标签（`user_id`, `tags_id_times`）
8. `merge_tag` 新旧标签 full join 后合并（`default_tag` 非空时，给本轮没算到的用户补该默认标签）
9. `write_2_df` upsert 写回 ES（`es.mapping.id = user_id`）
10. `spark.stop()`

**新增标签 = 继承 `TagBase` 且只实现 `compute(business_df, three_tag_df)`**，返回含 `user_id` + `tags_id_times` 两列的 DataFrame。

### `where_condition`：时间过滤的入口

`read_hive` / `execute` 新增了 `where_condition` 参数，会被拼进查询 SQL，形如 `select 字段 from 表 where 1=1 and <条件>`。条件在 `__main__` 里手工传：

```python
condition = "zt_id is not null and trade_date >= date_sub(current_date(), 90)"
obj.execute(two_tag_id=24, app_name="consumer_cycle", where_condition=condition)
```

⚠️ **它和元数据里的 `range` 字段还没有打通** —— `RuleParse` 解析出的 `range` 目前无人读取，条件要每个标签自己写。详见 `tags/statistics/CLAUDE.md`。

## 标签合并机制（第 8 步的实现细节）

`merge_tag` 用 `pandas_udf`（静态方法 `TagBase.merge_new_and_old_tagid`）合并新标签与 ES 中的旧标签逗号串：

先剔除旧串中属于**本次 2 级标签范围**的全部 3 级标签 ID（`all_three_tagid`），再与新结果并集去重、按数值排序。效果是同一标签重复跑不堆积，不同 2 级标签的结果相互保留。

需求文档 5.9 节把这个机制称为"核心难点"，并给出两种方案的取舍对比 —— 采用的就是这套"单字段 + 新老合并后覆盖"。

> ✅ 文档 5.9 要求的**"全量用户"语义已实现**（消费周期补 `30`、客户价值补 `46`），做法如下：
>
> - **`full join`**：`merge_tag` 本来就是 `how="full"`。
> - **`coalesce` 不需要额外写** —— join 用的是列名字符串 `on="user_id"`（USING 语义），结果里 `user_id` 只有一列，full join 下取值自动来自非空的那一侧。只有把关联条件写成 `new_df.user_id == old_df.user_id` 时才必须自己 `coalesce`。
> - **补默认值**：新增 `default_tag` 参数，链路是 `execute(..., default_tag="46")` → `merge_tag(..., default_tag)` → `merge_new_and_old_tagid` 的第 4 个参数。UDF 里先做兜底（本轮没算到就用默认值），再走原有的"剔除本轮 2 级标签范围的旧 id → 与新值并集去重排序"。
>   **`default_tag` 为空（不传）= 行为与改造前逐字节一致**，已用真值表验证 135 组，所以其余 11 个标签不受影响。
>
> 调用方：`tags/statistics/consumer_cycle.py`（`default_tag="30"`）、`tags/ml/value_tag.py`（`default_tag="46"`）。
>
> ⚠️ 语义取舍：全量标签下，"本轮算到了、但新值算出来是 null"的用户也会被补成默认值 —— 好处是全量语义更彻底，代价是映射逻辑万一出 bug 会被默认值掩盖、不报错。

## 两个实时基类（`StreamingETLBase` / `StreamingIndicateBase`）

与 `TagBase` 是**两套不同的体系**：`TagBase` 走"读 Hive → 打标签 → 写 ES"的批处理；
这两个走"读 Kafka → 加工 → 写 HDFS/Doris/Kafka"的流式处理。

### `StreamingETLBase`（`streaming_etl_base.py`）

| 方法 | 作用 |
|---|---|
| `create_SparkSession(appName)` | 建 SparkSession（**不整合 Hive**），checkpoint 统一在 `hdfs://up01:8020/streaming_chk` |
| `read_kafka(spark, topic, starting_offsets="earliest")` | 消费 Kafka，**默认从头读** |
| `convert_type(init_df)` | 把 `value` 由二进制转成字符串 |
| **`etl(cast_type_df)`（抽象）** | 子类实现，返回 `(etl_df, partition_field)` |
| `write_hdfs(etl_df, hdfs_path, partition_field)` | 写 ORC 到 `hdfs://up01:8020/xtzg/etl/{hdfs_path}`，按分区字段分目录 |
| `write_kafka(etl_df, topic)` | 转 JSON 写 Kafka（**带 `awaitTermination()`**） |
| `execute(appName, read_topic, write_topic=None, hdfs_path=None)` | 串起全流程 |

### `StreamingIndicateBase`（`streaming_indicate_base.py`）

| 方法 | 作用 |
|---|---|
| **`json_fields()`（抽象）** | 子类返回要从 JSON 解析出的字段名列表 |
| **`indicate(parse_df)`（抽象）** | 子类实现指标计算 |
| `json_parse(type_df)` | 按 `json_fields()` 用 `json_tuple` 解析 |
| `write_2_doris(result_df, table_name)` | `foreachBatch` 写 Doris（端口 **9030**） |
| `write_kafka(result_df, topic)` | 转 JSON 写 Kafka |
| `execute(appName, read_topic, write_topic=None, table_name=None)` | 串起全流程 |

### 两个已踩过的坑（写子类时注意）

- **`json_parse` 的字段名必须来自子类的 `json_fields()`** —— 不能拿 `type_df.columns`。
  那时 DataFrame 经 `convert_type` 只剩 `value` 一列，拿到的就是 `["value"]`，去 JSON 里找名为 `value` 的 key，什么都解析不出来。
- **`write_2_doris` 里的 `start()` 必须在 batch 函数外层** —— 写在 `_write_2_doris_fn` 内部会导致**每个 microbatch 都再启动一个新查询**，任务不断自我复制。

### ⚠️ checkpoint 必须用 `.option` 指定，不能用全局配置

三个实时基类都有类属性 `CHK_ROOT = "hdfs://up01:8020/streaming_chk"`，在各自的 `writeStream` 上用
`.option("checkpointLocation", f"{CHK_ROOT}/{appName}_xxx")` 显式指定落点。

**为什么不用 `spark.sql.streaming.checkpointLocation`（SparkSession 全局配置）**：
Spark 遇到全局配置时，会在该路径下**再拼一个每次启动都不同的随机 UUID**：

```
全局配置：  hdfs://.../streaming_chk/{每次启动都不同的 UUID}   ← 路径不固定 ✗
.option：  hdfs://.../streaming_chk/{appName}_es            ← 路径固定 ✓
```

路径不固定 = 每次重启读不到旧进度 = **每次都从头消费**，后果随 `startingOffsets` 而变：

| `startingOffsets` | 表现 |
|---|---|
| `earliest` | 每次全量重放 —— 幂等下游（Doris / ES）无害，但 `append` 语义的出口（HDFS、Kafka）会堆重复数据 |
| `latest` | **静默漏数据** —— 任务停机期间的消息永久丢失，不报错不告警 |

**一个任务有多个 sink 时，每个 sink 要各占一个 checkpoint 目录**
（`nginx_log_etl` 同时写 HDFS 和 Kafka，是两个独立的流式查询）：

| 基类 | checkpoint 目录 |
|---|---|
| `StreamingETLBase` | `{CHK_ROOT}/{appName}_hdfs`、`{CHK_ROOT}/{appName}_kafka` |
| `StreamingIndicateBase` | `{CHK_ROOT}/{appName}_doris`、`{CHK_ROOT}/{appName}_kafka` |
| `StreamingTagBase` | `{CHK_ROOT}/{appName}_es` |

> `startingOffsets` **只在 checkpoint 不存在（首次启动）时起作用**。之后由 checkpoint 记录的 offset 接管 ——
> 这也是"生产环境可以把 `earliest` 去掉"能成立的前提：落点固定后，它只管第一次。

### 与 `TagBase` 的三个区别

- **不继承 `TagBase`**，是各自独立的抽象基类
- **不读元数据**：topic 名、输出路径都由 `execute()` 参数传入
- **没有标签合并机制**：实时标签每次覆盖写（`conversion_rate.py` 会从元数据读 Kafka 配置，是三个实时基类中唯一这么做的）

## 文档规划 vs 实际（差异记录）

需求文档 9.6 节规划的基类是 **4 个**，实际实现了 3 个（文件名/类名与规划不完全一致）：

| 文档规划 | 实际 |
|---|---|
| `tags/base/tags_base.py` → `AbstractTagsBase` | `tags/base/tags_base.py` → `TagBase`（文件名已对齐，类名不同） |
| `streaming_etl_base.py` → `AbstractStreamingEtlBase` | `streaming_etl_base.py` → **`StreamingETLBase`**（文件名对齐，类名少了 `Abstract`） |
| `streaming_analysis_base.py` → `AbstractStreamingAnalysisBase` | `streaming_indicate_base.py` → **`StreamingIndicateBase`**（**文件名和类名都不同**：`analysis` → `indicate`） |
| `streaming_tags_base.py` → `AbstractStreamingTagsBase` | `streaming_tag_base.py` → **`StreamingTagBase`**（文件名少了复数 `s`，类名少了 `Abstract`） |

文档规划的 `AbstractTagsBase` 接口比现在的 `TagBase` 宽得多：

- 抽象方法：`get_parameter()`、`compute()`
- 通用方法：`execute`、`read_parameter`、`save_result`、`update_tags`、`write_es`(静态)、`read_es`(静态)、`read_rule_5`、`read_hive`、`read_rule_4`、`get_mysql`(静态)、`get_spark`(静态)
- `get_parameter` 返回值约定：`app_name`(str)、`rule_id`(int)、`condition`(可选)、`tags_field`(可选，默认 `tags_id_times`)

注意 `get_spark` / `read_es` 是**静态方法**，因为 `tags/recommend/` 下的推荐脚本要直接调用它们而不实例化标签类。

**开发新标签类型时，先决定是扩展 `TagBase` 还是按文档补一个新基类，别直接改 `TagBase` 破坏已有的匹配类标签。**
