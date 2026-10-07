# tags/bean — 数据实体类

> 包级说明。项目全局约定（运行环境、集群地址、通用陷阱）见项目根 `CLAUDE.md`。

## 职责

存放数据实体类（POJO），即用来承载解析结果的对象。

## 现状：空目录

只有 `__init__.py`（内容为 `"""数据类"""`），**没有任何实体类**。

### ⚠️ 已存在的错位（如实记录）

需求文档 9.6 节规划这里应该放 `rule_meta.py` → `RuleMeta` 类，但**实际代码把解析逻辑写在了 `tags/utils/rule_parse_util.py` 的 `RuleParse` 类里**。

| 文档规划 | 实际 |
|---|---|
| `tags/bean/rule_meta.py` → `RuleMeta` | 不存在 |
| 字段：`inType`、`nodes`、`table`、`selectFields`、`range` | `RuleParse` 有相同的 5 个属性 |
| 方法：`from_rulestr_to_esmeta(rule_str)` | `RuleParse.parse(rule)` 做同样的事 |

两者功能完全等价，只是**放错了包、改了名**。文档 5.4–5.7 各章节里写的 `RuleMeta.from_rulestr_to_esmeta(rule_str)` 指的就是现在 `RuleParse.parse(rule)` 这个动作。

后续若要按文档规整工程，应把它迁到本包并改名 —— 但迁移前要同步改掉 `tags/base/tags_base.py` 里的 `from tags.utils.rule_parse_util import RuleParse` 及全部调用点。

## 标签元数据驱动（本包最相关的背景）

标签的**表来源和匹配规则不在代码里，而在 MySQL `tbl_basic_tag` 表**。实际存档见 **`data/mysql/tags_info.sql`**（102 条记录），**开发新标签前必读**。

标签按 `level` 分三层，读元数据前先理解层级：

| level | 含义 | 例（`id`） |
|---|---|---|
| 1 | 标签分类 | 人口属性（3）、商业属性（38）、行为属性（77） |
| 2 | **具体标签** | 性别（4）、年龄段（15）、RFM（113） |
| 3 | **标签值** | 男 / 女 / 未知（5/6/7）、50后~20后（16–23） |

### rule 串格式

- **2 级标签**的 `rule` 是 `##` 分隔的 key=value 串，解析为 `inType/nodes/table/selectFields/range` 五个属性，决定读哪张 Hive 表、取哪些字段。例：
  `inType=Hive##nodes=up01:9083##table=dwd.dwd_mem_member_union_i##selectFields=zt_id,sex##range=all`
  `inType` 有两类：`Hive`（离线，主体）和 `Kafka`（实时，如转化率 id=123、近期活跃度 id=127）。
- **3 级标签**的 `rule` 是该标签的取值区间或枚举（年龄段如 `19900101-19991231`、性别如 `1`），它的 `id` 就是最终写进 `tags_id_times` 的标签 ID。

> 注意 `tags/utils/rule_parse_util.py:38` 的示例串把表名拼成了 `dwd_mem_menber_union_i`（少一个 `m`），真实元数据是 `dwd_mem_member_union_i`。**别照抄那行**。

### 解析的脆弱点

`RuleParse.parse()` 用 `RuleParse(**new_dict)` 构造实例，这意味着 rule 串里**五个 key 一个都不能少**，缺任何一个直接 `KeyError`。改元数据时务必保证 key 完整。
