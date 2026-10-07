# tags/ml — 挖掘类标签

> 包级说明。项目全局约定（运行环境、集群地址、通用陷阱）见项目根 `CLAUDE.md`；
> 基类流水线见 `tags/base/CLAUDE.md`；元数据 rule 格式见 `tags/bean/CLAUDE.md`。

## 职责

用**机器学习算法**对数据分类或预测后贴标签。

## 现状：已实现 2 个（客户价值、PSM），USG 的挖掘版已放弃

| 文件 | 类名 | rule_id | 标签 | 算法 | 状态 |
|---|---|---|---|---|---|
| `value_tag.py` | `ValueTag` | 39 | 客户价值 | K-Means | ✅ 已实现 |
| `psm_ml.py` | `PSMMl` | 52 | 价格敏感度 | K-Means | ✅ 已实现 |
| —— | —— | 58 | 购物性别 | 决策树 CART | ❌ **已放弃，不做** |

> **命名不一致**：文档 5.6 规划 `value_tags.py` / `ValueTags`、`kmeans_psm_tags.py` / `KMeansPSMTags`，
> 实际是 `value_tag.py` / `ValueTag`、`psm_ml.py` / `PSMMl` —— 既不统一带复数 `s`，也没沿用 `kmeans_` 前缀。**以代码为准**。

> ✅ **策略已定：ml 与 tag 两条路线并存**（既不是"替换"，也不再"暂缓"）。
> PSM(52) 因此有**两个实现**：统计类 DSL 路线 `tags/statistics/psm_tag.py`（三个比率求和后区间匹配）
> 与挖掘类聚类路线 `tags/ml/psm_ml.py`（三个比率做特征、K-Means 分 5 簇）。
>
> ⚠️ **并存的主要风险是互相覆盖**：两条路线都执行 `two_tag_id=52`、都写 ES 同一列 `tags_id_times` 的同一批 3 级标签 id（53–57），
> 而 `TagBase.merge_tag` 的规则是"先剔除本 2 级标签范围的旧 id，再写新值" —— 也就是**后跑的覆盖先跑的**，最终值取决于调度顺序。
> 并存期间不要让两条路线写同一个字段（或明确只投产一条）。详见 `tags/statistics/CLAUDE.md`。

> ❌ **USG(58) 的挖掘版（决策树 CART）已决定不做**。它是有监督**分类**、不是聚类：标签是 男1/女2/中性0，
> 既没有"可求和的连续分数"，id 之间也没有顺序含义 —— 本包那套「聚类中心求和降序 ↔ 标签 id 升序」的映射**对它本来也不适用**。
> 统计类的 `tags/statistics/usg_tag.py` 保留，是 USG 的唯一实现。

这三个 `rule_id` 与 `data/mysql/tags_info.sql` 里 `tbl_model` 表的 `tag_id` 完全对应，可交叉验证。

## 客户价值的实际实现（`value_tag.py`）

与文档 5.6 的差异（**以代码为准**）：

| 项 | 代码实际 | 文档 5.6 |
|---|---|---|
| 文件名 / 类名 | `value_tag.py` / `ValueTag` | `value_tags.py` / `ValueTags` |
| `seed` | `1` | `666` |
| 模型路径 | `/spark_ml/kmeans` | `/model/ValueModel` |
| 模型判存 | `HDFSUtil.isexists(path)` | `HdfsUtil().exists(path)` |
| `app_name` | `value_tag` | — |

实现要点：

- 三个指标从**订单表字段**算（元数据 id=39 的 `selectFields` 是 `zt_id,trade_date,order_no,real_paid_amount`，过滤条件 90 天）：
  R = `datediff(current_date(), trade_date)` 取 `min`、F = `count(order_no)`、M = `sum(real_paid_amount)`。
- 打分用 `when/otherwise`，**每段最后统一用 `otherwise` 收口**，避免区间之间留下空档得 null。
- `VectorAssembler` 组三个得分 → 模型存在则 `KMeansModel.load("hdfs://192.168.88.166:8020" + path)`，否则 `KMeans(k=7)` `fit` 后 `save`。
- 聚类编号 → 标签 id：聚类中心的 R/F/M 得分**求和降序**得到价值高低顺序，再与 `three_tag_df.orderBy("id")` 的 id 依次 `zip`。
  ⚠️ **必须显式 `orderBy("id")`** —— `collect()` 不保证顺序，标签 id 顺序一变就会**静默错配**（标签打反却不报错）。该映射还依赖 `k` 与 3 级标签数量一致，改 `k` 时要同步。
- 映射 UDF 用 `dict[prediction]`（`[]` 而非 `.get()`）：映射不全时直接抛 `KeyError` 及早暴露，而不是产出一批 null 标签。
- 复用已 `transform` 过的 `kmeans_result`，不要再 `transform` 一次（那是白算一遍）。
- 三处中间结果的 `print`（聚类中心、排序后的聚类编号、聚类编号→标签 id 的字典）目前是**注释掉**的，与 `psm_ml.py` 一致。第一次上集群时建议临时放开，即可对着文档核对映射 `{1:40, 6:41, 2:42, 3:43, 5:44, 4:45, 0:46}`。

> ✅ **"全量用户"语义已补齐** —— 文档 5.9 要求客户价值给未覆盖用户补默认标签 `46`（超低价值），
> 现在 `__main__` 里传了 `default_tag="46"`：近 90 天没下单、本轮算不到的用户会被补成 46，
> 而不是一直停在上一轮的旧值上。机制见 `tags/base/CLAUDE.md`。

## PSM 的实际实现（`psm_ml.py`）

与文档 5.6 的差异（**以代码为准**）：

| 项 | 代码实际 | 文档 5.6 |
|---|---|---|
| 文件名 / 类名 | `psm_ml.py` / `PSMMl` | `kmeans_psm_tags.py` / `KMeansPSMTags` |
| `seed` | `12` | `666` |
| 模型路径 | `/spark_ml/psm` | `/model/PSMModel` |
| 模型判存 | `HDFSUtil.isexists(path)` | `HdfsUtil().exists(path)` |
| `app_name` | `psm_ml` | — |

实现要点：

- 前三步（取字段 → 按 `zt_id` 聚合四个基础量 → 算 `tdonr`/`adar`/`tdar`）与 `tags/statistics/psm_tag.py` **逐行相同**，`state` 口径也一致（`discount_amount > 0`）。
- 分叉在最后一步：统计类把三个比率**相加成一个 `psm`** 再匹配区间；本文件**不求和**，把三个比率直接当三个特征交给 `KMeans(k=5)`。
- `psm_df = rate_df.fillna(0)` 处理"一单优惠都没用过"导致的 `0/0` 空值。
- ⚠️ **故意不做钳制**：`psm_tag.py` 要跟区间比大小，必须 `greatest/least` 把 psm 钳到 `[0, max_end]`；聚类路线不跟上下界比较，**不需要**那两行 —— 不是漏写。
- 聚类编号 → 标签 id：聚类中心的三个分量**求和降序**得到敏感度高低顺序，再与 `three_tag_df.orderBy("id")` 的 id（53–57）依次配对。
  同样**必须显式 `orderBy("id")`**，同样依赖 `k`（5）与 3 级标签数量一致。
- 映射 UDF 用 `dict[prediction]`（`[]` 而非 `.get()`），映射不全时直接抛 `KeyError`。
- 中间结果的 `print` 目前是**注释掉**的（`value_tag.py` 也一样）。第一次上集群时建议临时放开，把实际映射与文档 5.6 的 `{2:53, 3:54, 4:55, 0:56, 1:57}` 对一遍。

> ⚠️ **模型一旦落盘就永久复用**：`isexists → load` 意味着第一次跑成功后 `/spark_ml/psm` 就存在了，之后**永远走 `load`、不再训练**。
> 所以：① 第一次跑之前先确认该路径不存在（或先删）；② 如果映射和预期不符，**光重跑没用**，要先 `hdfs dfs -rm -r /spark_ml/psm` 才能重训。
> 同一个坑存在于 `value_tag.py` 的 `/spark_ml/kmeans`。

> ⚠️ **预期数据面**：实测只有约 16.4% 的订单用了优惠，所以"三个特征全 0"（从不用优惠）的用户可能非常多，
> K-Means 容易把一大簇人聚成全零簇 —— 与 USG 的"99% 中性"属同类数据源现象。上集群后先看簇分布，别急着怀疑代码。

## ⚠️ 关键业务约束（文档 5.6，最容易做错的地方）

针对**客户价值**标签：

- **不能用聚类编号直接当标签** —— 聚类编号（0~6）本身没有业务含义；
- **必须按聚类中心的业务含义建立映射**：对聚类中心求和、降序排序后，再与 3 级标签 id 依次对应；
- **不能用 RFM 三值直接相加判断价值** —— `(5,5,1)`、`(1,5,5)`、`(5,1,5)` 相加都等于 11，但业务含义完全不同，必须用聚类区分。

## 三个标签的算法与参数（文档 5.6 的原始规划，其中 USG 已放弃）

### 客户价值（K-Means，rule_id=39）

- **特征构造**：先算 RFM 三项并按规则打分（打分后已相当于做过 MinMaxScaler，**无需再做归一化**）
  - R：`<3`→5分、`3-6`→4、`6-10`→3、`10-15`→2、否则 1
  - F：`≥32`→5、`24-32`→4、`16-24`→3、`8-16`→2、否则 1
  - M：`≥900`→5、`675-900`→4、`450-675`→3、`225-450`→2、否则 1
- 用 `VectorAssembler` 组特征 → `KMeans(k=7, seed=666)`
- **聚类中心求和降序排序后与 3 级标签 id 依次对应**，映射结果：
  `{1:40, 6:41, 2:42, 3:43, 5:44, 4:45, 0:46}`
- 3 级标签：超高价值1 … 超低价值7（id 40–46）
- 模型路径：`/model/ValueModel`

### 价格敏感度 PSM（K-Means，rule_id=52）

- 特征：`tdonr`、`adar`、`tdar` 三个字段（**不再求和**，这一点与统计类口径不同）
- `KMeans(k=5, seed=666)`
- 映射：`{2:53, 3:54, 4:55, 0:56, 1:57}`
- 模型路径：`/model/PSMModel`

### 购物性别 USG（决策树 CART，rule_id=58）

> ❌ **已放弃，不实现。** 下面只是需求文档 5.6 的原始参数，留作参考。

- **12 个特征**：`total`、`male_count`、`female_count`、`total_order_count`、`total_amount`、`total_discount`、`male_rate`、`female_rate`、`everage_goods_count`、`everage_amout`、`discount_order_rate`、`discount_amount_rate`（注意文档里 `everage_*` 就是拼写如此）
- 空值 `fillna(0.0)`
- 标注来源：会员表有效性别（`sex in (1,2)`）
- `DecisionTreeClassifier(maxDepth=5, seed=666)`；`randomSplit([0.8,0.2], seed=666)`
- 评估：`MulticlassClassificationEvaluator(metricName='accuracy')`
- 模型路径：`/model/USGModel`
- 3 级标签：男1（59）、女2（60）、中性0（61）

## 模型复用逻辑（已落地）

先用 `HDFSUtil.isexists(path)` 判断模型是否存在 → 存在则 `load`，不存在则训练并 `save("hdfs://up01:8020" + path)`。

`HDFSUtil` 位于 **`tags/utils/hdfs_util.py`**（✅ 已实现），基于 `pyhdfs.HdfsClient`：
`hosts='up01:9870'`、`user_name='root'`。注意端口是 **9870**（HDFS NameNode Web UI），不是 8020。

> **类名与方法名和文档不一致**：文档写 `HdfsUtil().exists()`，实际是 **`HDFSUtil.isexists()`**（类名 HDFS 全大写、方法名多个 `is` 前缀），调用时直接用类名，不要实例化（utils 约定见根 `CLAUDE.md`）。
>
> ⚠️ `isexists` 一度漏写 `return`，导致永远返回 `None`、`load` 分支永不进入（模型持久化形同虚设，且不报错只是白跑重训）—— 现已修复。以后写这类"判存"方法记得返回值。

> 与 `tags/recommend/` 的区别：推荐系统只有 FP-Growth 持久化模型；本包**已实现的两个**模型（客户价值、PSM）都走 HDFS 复用。
