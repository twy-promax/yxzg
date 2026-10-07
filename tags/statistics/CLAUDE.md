# tags/statistics — 统计类标签

> 包级说明。项目全局约定（运行环境、集群地址、通用陷阱）见项目根 `CLAUDE.md`；
> 基类流水线见 `tags/base/CLAUDE.md`；元数据 rule 格式见 `tags/bean/CLAUDE.md`。

## 职责

需按用户 ID **分组聚合**后才能贴的标签。需求文档 5.5 节称其为**重点开发对象、更新最频繁**。

## 现状：已实现 8 个（按元数据口径，统计类已全部完成）

| 标签 | rule_id | 路线 | 文件 | 状态 |
|---|---|---|---|---|
| 消费周期 | 24 | DSL 系 | `consumer_cycle.py`（`ConsumerCycle`） | ✅ 已实现 |
| 支付方式 | 31 | DSL 系 | `payment.py`（`Payment`） | ✅ 已实现 |
| 活跃度 | 47 | DSL 系 | `activity_tag.py`（`ActivityTag`） | ✅ 已实现 |
| 价格敏感度 PSM | 52 | DSL 系 | `psm_tag.py`（`PSMTag`） | ✅ 已实现 |
| 购物性别 USG | 58 | DSL 系 | `usg_tag.py`（`USGTag`） | ✅ 已实现 |
| 客单价 | 78 | DSL 系 | `unit_price_tag.py`（`UnitPriceTag`） | ✅ 已实现 |
| 新老会员 | 85 | SQL 系 | `new_old_user.sql` | ✅ 已实现 |
| RFM | 113 | SQL 系 | `rfm.sql` | ✅ 已实现 |

### 只有元数据里存在的标签才能开发

元数据 `tbl_basic_tag` 里属于统计类的 2 级标签**一共就上面 8 个，已全部实现**。

需求文档 5.5 另规划了 5 个比率类标签（退货率 / 换货率 / 差评率 / 客诉率 / 赔付率），
但**元数据里没有任何对应的 2 级标签**，所以做不了 —— 要做得先在 `tbl_basic_tag` 建档。

> 别混淆：同属"商业 / 行为属性"、但**不属于统计类**的还有三个 ——
> `客户价值`(39) 归挖掘类（`tags/ml/`），`转化率`(123) 与 `近期活跃度`(127) 归实时类（`tags/streaming/`）。

> ⚠️ **USG 的数据源缺陷（不是代码问题）**：USG 依赖的特征编码中有 **10 个在现有数据源里完全不存在** ——
> 品类 `20020304`、`20020201`、`20020801`、`20020102`、`20020402`、`30050301`、`30050401`、`30020901`，
> 商品 `3231330`、`3216901`（数据中共有 248 个三级品类、1669 个商品，均查无此编码）。
> 实测结果是约 **99% 的用户被打成"中性"**。排查该标签时不要往代码方向找。

> **USG 的时间范围以元数据为准**：源表是 `dwm_sold_goods_sold_dtl_i`（订单商品明细，一行 = 一个商品），
> 元数据 `rule` 里是 `range=90`，所以取近 **90** 天。需求文档 5.5 写的"实际取 180 天"与元数据不一致，**以元数据为准**。

> **PSM 有三个特殊之处**（细节见 `psm_tag.py` 的方法注释）：
>
> 1. **3 级标签的 rule 用 `~` 分隔**（如 `0.6~1`），不是其他标签的 `-`，
>    调用 `by_range` 时必须传 `rule_sep="~"`，否则切不出 start/end。
> 2. **相邻区间共享端点**（`0.6~1` 与 `1~3` 都含 1），闭区间下一个 psm 值会同时命中两条，
>    必须传 `keep_leftmost=True` 只保留最左侧那条 —— 实测有 3029 人会被重复打标。
> 3. **psm 会三种越界**，都要钳制：分母为 0 时算出 null（`fillna(0)`）、
>    优惠金额为负时 psm < 0（`greatest(...,0)`）、`adar > 1` 时超出上界（`least(..., max_end)`）。
>    其中**上界从元数据动态取**（当前是 `1~3` 的 3），硬编码会在元数据调整后留下匹配不上的空档。

> **命名不一致**：需求文档 5.5 节写 `buying_cycle_tags.py` / `BuyingCycleTags`，实际是 `consumer_cycle.py` / `ConsumerCycle`。**以代码为准**。

## 时间范围过滤：靠 `where_condition` 手工传入

`TagBase.read_hive` 和 `execute` 都新增了 `where_condition` 参数，会被拼进查询 SQL：

```python
condition = "zt_id is not null and datediff(current_date(),trade_date)<=90"
obj.execute(two_tag_id=24, app_name="consumer_cycle", where_condition=condition)
```

⚠️ **元数据里的 `range` 字段仍未接入这条链路** —— `RuleParse` 解析出的 `range`（`all`/`90`/`30`/`earliest`）目前没有任何代码读取，时间条件要每个标签自己在 `__main__` 里手写。也就是说**改元数据的 `range` 值不会影响任何标签的计算范围**。

`range` 的四种取值及其出现位置：

| 取值 | 含义 | 出现于 |
|---|---|---|
| `all` | 全量，无需过滤 | 匹配类标签（性别、职业、年龄段、政治面貌、婚姻状况、国籍） |
| `90` / `30` | 近 N 天 | **本包全部标签**：消费周期、支付方式、客单价、活跃度、（统计口径的）PSM、USG、RFM、新老会员 |
| `earliest` | Kafka 从头消费 | 实时标签（见 `tags/streaming/`） |

若要统一收口，应在 `parse_tag_rule` 之后按 `range` 自动生成 `where_condition`，而不是继续让每个标签手写。

## 本包通用的两个工具类

`tags/utils/` 下这两个类覆盖了统计类标签最常见的收尾动作，**直接用类名调用，不要实例化**（utils 编码约定见根 `CLAUDE.md`）：

| 类 | 方法 | 用途 |
|---|---|---|
| `ThreeTagIdJoiner` | `by_range(df, three_tag_df, value_col)` | 按数值区间关联，如消费周期的 `day_diff`、PSM 的 `psm`。另有 `rule_sep`、`keep_leftmost` 两个可选参数，用途见 `tags/utils/three_tag_id_joiner.py` |
| `ThreeTagIdJoiner` | `by_equal(df, three_tag_df, value_col)` | 按值相等关联，如支付方式的 `paytype` |
| `ThreeTagIdMapper` | `to_udf(three_tag_df, ...)` | 用 UDF 把字段值映射成标签 id（枚举查表） |

## 标签清单与计算口径（文档 5.5）

| 标签 | 源表（`selectFields`、`range`） | 计算口径 | 3 级标签（元数据 id） |
|---|---|---|---|
| 消费周期 | `dwm.dwm_sell_o2o_order_i`（`zt_id,trade_date`，90） | 近 90 天 → 按 `zt_id` 分组 `max(trade_date)` → `datediff(current_date, trade_date)` → 区间匹配 | 近7天0-7、近14天8-14、近30天15-30、近60天31-60、近90天61-90、90天以上91-36500（25–30）；**本轮没算到（近 90 天无单）补 `30`** |
| 支付方式 | `dwm.dwm_sell_o2o_order_i`（90） | 按**支付次数**（非金额）：取每单金额最大的渠道为该单方式 → 按 `zt_id,pay_type` 计数 → 开窗 `row_number()` 取 `rn==1` | 支付宝、微信、现金、余额、银联、其他（32–37） |
| 客单价 | `dwm.dwm_sell_o2o_order_i`（`zt_id,order_no,real_paid_amount`，90） | 90 天内平均每单价格 | 超低0-5、低5-15、中等15-30、高30-60、超高60-100000（79–83） |
| 活跃度 | `dwd.dwm_sell_o2o_order_i`（元数据源表见下） | 90 天内消费次数 | 非常活跃30-10000、活跃15-30、不活跃5-15、非常不活跃0-5（48–51） |
| RFM | `dwm.dwm_mem_member_behavior_day_i`（`zt_id,consume_times,consume_amount`，30） | R：最近一次下单距今 ≤7 天记 1；F：频次 ≥ 均值记 1；M：花费 ≥ 均值记 1。仅分析最近一月有下单用户 | 111→114、101→115、011→116、001→117、110→118、100→119、010→120、000→121 |
| 新老会员 | `dwm.dwm_mem_first_buy_i`（`zt_id,dt`，30） | 不在首购表→88；在表内但不在近 30 天→87；否则→86 | 新会员1（86）、老会员2（87）、未消费3（88） |
| PSM 价格敏感度 | `dwm.dwm_sell_o2o_order_i`（`zt_id,order_no,order_total_amount,discount_amount,real_paid_amount`，90） | `psm = tdonr + adar + tdar`；`state`：**代码用 `discount_amount > 0`**（需求文档 5.5 写的是 `real_paid_amount==0 → 0`，**以代码为准**，见下）；分母为 0 产生 null 需 `fillna(0)` | 极度敏感1~3、比较敏感0.6~1、一般敏感0.4~0.6、不太敏感0.2~0.4、极度不敏感0.0~0.2（53–57） |
| USG 购物性别 | `dwm.dwm_sold_goods_sold_dtl_i`（90，**实际取 180 天**） | 特征品类/商品加权算男女倾向率，阈值 0.01 | 男1（59）、女2（60）、中性0（61） |
| 退货/换货/差评/客诉/赔付率 | 行为数据、订单表、订单商品表、客诉表、评价表 | 按各自分母算比率后分档 | 超高、高、中等、低、很低 |

> ⚠️ **`state` 口径：需求文档与代码不一致** —— 需求文档 5.5（第 415 行）写 `real_paid_amount==0 → 0` 否则 1；
> 而 `psm_tag.py`（第 54 行）与 `tags/ml/psm_ml.py`（第 57 行）**用的都是 `discount_amount > 0`**。
> 它直接决定 `tdon`（优惠订单数）怎么数，会整体改变 psm 的取值。**当前以代码为准**；若将来要回归文档口径，两个文件必须同时改。

> ✅ **消费周期是"全量用户"标签**：`consumer_cycle.py` 的 `__main__` 传了 `default_tag="30"` ——
> 近 90 天没有下单、本轮算不到的用户会被补成 `30`（90天以上），而不是保留上一轮的旧值。
> 机制（`full join` + 补默认值）见 `tags/base/CLAUDE.md` 的「标签合并机制」。

> ✅ **策略已定：两条路线并存** —— 文档 5.5 把它们当统计类、5.6 又当挖掘类，元数据 `tbl_model` 里 `tag_id=52`(PSM)、`tag_id=58`(USG) 都配了算法。
> 现状：**PSM(52) 两侧都已实现** —— 本包 `psm_tag.py`（三个比率求和后区间匹配）+ `tags/ml/psm_ml.py`（三个比率做特征、K-Means 分 5 簇）；
> **USG(58) 目前只有本包 `usg_tag.py`**，挖掘类的决策树版未开发。
>
> ⚠️ **并存会互相覆盖**：`psm_tag.py` 与 `psm_ml.py` 都执行 `two_tag_id=52`、都写 ES 同一列 `tags_id_times` 的同一批 id（53–57），
> 而 `TagBase.merge_tag` 先剔除本 2 级标签范围的旧 id 再写新值 —— **后跑的覆盖先跑的**，最终值取决于调度顺序。
> 并存期间别让两条路线写同一字段，或明确只投产一条。详见 `tags/ml/CLAUDE.md`。

> **USG 特征品类/商品清单**（男女各 6 品类 + 5~6 商品，具体编码）见需求文档 5.5 节第 421–424 行，清单较长不在此重复。

## 两条实现路线（文档 5.5 开发要点）

| 路线 | 已实现标签 | 做法 |
|---|---|---|
| **DSL 系** | 消费周期、支付方式、PSM、USG | 沿用 `TagBase` 流水线：读 Hive → 处理 → 分组聚合 → 匹配 3 级标签 → 读历史 ES → 新老合并 → 写 ES |
| **SQL 系** | 新老会员、RFM | 在 Hive 建 `ads` 库结果表，于 **Spark Thrift Server**（`up01:10001`）上用 SQL 计算，再经 SeaTunnel 送 ES |

### SQL 系的完整链路（已跑通）

| 步骤 | 文件 | 说明 |
|---|---|---|
| 1. 算新老会员 | `tags/statistics/new_old_user.sql` | 首购表 `dwm_mem_first_buy_i` left join 订单表，按 `day_diff` 分档（≤30→86 新会员 / >30→87 老会员 / 无→88 未消费）→ `ads.ads_mem_new_old_user_i` |
| 2. 算 RFM | `tags/statistics/rfm.sql` | 4 层 CTE：R 取最小时间差 `<7`、F/M 与均值比较，各转 0/1 后按组合映射到 114–121 → `ads.ads_mem_user_rfm_i` |
| 3. 合并 | `tags/statistics/sql_user_tag.sql` | 把上面两张结果表 `union all` 后按用户 `collect_list` 合并成一行 → `ads.ads_mem_tags_i` |
| 4. 送 ES | `scripts/seatunnel_scripts/hive2es.config` | SeaTunnel 按 `read_partitions` 读 `ads.ads_mem_tags_i` 的指定分区 → 写 `user_profile_tags` |

**第 4 步的字段映射**（`hive2es.config` 的 `FieldMapper`）：`tags_id` → ES 的 **`tags_id_once`** 字段。
即 SQL 系标签落在 ES 的"一次性"列上 —— 另外两列 `tags_id_times`（离线标签，`TagBase` 流水线写的）和 `tags_id_streaming`（实时标签）各走各的。

> ⚠️ **两个计算的 `partition(dt)` 写法不一致**：`sql_user_tag.sql` 显式写了 `partition(dt)`，
> 而 `new_old_user.sql`、`rfm.sql` 都**没写**（尽管目标表是分区表）。
> 三者在 Spark Thrift Server 上都能跑通 —— **Spark 会自动把 SELECT 的最后一列当成分区值**。
> 但 Hive SQL 不认这种省略（会报 `Partition spec is required`）。
> 为避免换引擎或换人维护时踩坑，建议统一补上 `partition(dt)`。

### SQL 系的结果表与合并（关键）

```sql
create table ads.ads_mem_user_rfm_i    (user_id bigint, tags_id string) partitioned by (dt string);
create table ads.ads_mem_new_old_user_i(user_id bigint, tags_id string) partitioned by (dt string);
create table ads.ads_mem_tags_i        (user_id bigint, tags_id string) partitioned by (dt string);
```
（均 ORC + SNAPPY）

> 建表脚本已就绪：`scripts/hive_scripts/hive_ads_create_table.sql`（建 `ads` 库 + 上述 3 张表）。
> ⚠️ 该脚本第 3 行是一句 `drop table ads.ads_mem_new_old_user_i;` —— **没有 `if exists`**，而且只 drop 了 3 张中的 1 张。表不存在时会直接报错，确认是有意为之还是笔误。

**合并 SQL**（把所有 SQL 系标签汇总成一张表再导入 ES）：

```sql
insert overwrite table ads.ads_mem_tags_i partition(dt)
select user_id, concat_ws(',', collect_list(tags_id)) as tags_id, '${inputdate}' as dt
from (select * from ads.ads_mem_user_rfm_i     where dt = '${inputdate}'
      union all
      select * from ads.ads_mem_new_old_user_i where dt = '${inputdate}') t
group by user_id;
```

> ⚠️ **Spark SQL 算出的标签导入 ES 时会直接覆盖原标签**，所以必须先把所有 SQL 计算的标签**汇总合并后一起导入**（即上表的 `ads_mem_tags_i`）。这是本包最容易造成线上事故的点。

> 若将来还有标签需要"全量用户"语义：**走 `TagBase` 流水线的（DSL 系）基类已支持** ——
> `execute(..., default_tag=...)` 即可（消费周期用 `30`、客户价值用 `46`）；
> **SQL 系**（新老会员、RFM）没有这条路，要在合并 SQL 里自己 `full join` + 补默认值。详见 `tags/base/CLAUDE.md`。
