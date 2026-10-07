# tags/match — 匹配类标签

> 包级说明。项目全局约定（运行环境、集群地址、通用陷阱）见项目根 `CLAUDE.md`；
> 基类 `TagBase` 的流水线见 `tags/base/CLAUDE.md`；元数据 rule 格式见 `tags/bean/CLAUDE.md`。

## 职责

数据与规则**直接匹配**即可贴上的标签（人口属性为主）。复杂度最低，**6 个标签已全部实现**。

## 现状：6 个全部已实现

| 标签 | rule_id | 文件 | 类名 | 源字段 |
|---|---|---|---|---|
| 性别 | 4 | `gender_tag.py` | `GenderTag` | `sex` |
| 年龄段 | 15 | `age_tag.py` | `AgeTag` | `birthday_date` |
| 职业 | 8 | `job_tag.py` | `JobTag` | `job` |
| 政治面貌 | 63 | `politics_tag.py` | `PoliticsTag` | `politics` |
| 婚姻状况 | 67 | `marital_tag.py` | `MaritalTag` | `marital_status` |
| 国籍 | 71 | `nation_tag.py` | `NationTag` | `address` |

> **命名不一致**：需求文档 5.4 节写 `gender_tags.py`/`GenderTags`、`age_tags.py`/`AgeTags`、`job_tags.py`/`JobTags`（带复数 `s`），实际代码是 `xxx_tag.py`/`XxxTag`（不带 `s`）。**以代码为准**。

## 各标签开发要求（文档 5.4）

| 标签 | 源表（`selectFields`） | 规则要点 | 3 级标签 |
|---|---|---|---|
| 性别 | `dwd.dwd_mem_member_union_i`（`zt_id,sex`） | 空值填 `sex='0'` | 男=1（id 5）、女=2（id 6）、未知=0（id 7） |
| 年龄段 | `dwd.dwd_mem_member_union_i`（`zt_id,birthday_date`） | 生日转 `yyyymmdd` 后按区间匹配 | 50后=19500101-19591231 … 20后=20200101-20291231（id 16–23） |
| 职业 | `dwd.dwd_mem_member_union_i`（`zt_id,job`） | 空值 `fillna('7','job')` | 学生1 / 公务员2 / 军人3 / 警察4 / 教师5 / 白领6（id 9–14） |
| 国籍 | `dwd.dwd_mem_member_union_i`（元数据用 `zt_id,address`） | 数据是中文，与 rule 数据类型不同，**需转换** | 中国大陆1 / 中国香港2 / 中国澳门3 / 中国台湾4 / 其他5（id 72–76） |
| 婚姻状况 | `dwd.dwd_mem_member_union_i`（`zt_id,marital_status`） | 空值按"未婚"处理 | 未婚1 / 已婚2 / 离异3（id 68–70） |
| 政治面貌 | `dwd.dwd_mem_member_union_i`（`zt_id,politics`） | 团员归属"群众" | 群众1 / 党员2 / 其他党派3（id 64–66） |

**源表字段名以元数据 `selectFields` 为准**（上面已按 `data/mysql/tags_info.sql` 实际值标注），不要凭标签名猜字段。

> 拉链表过滤条件统一为 `and end_date = '9999-99-99'`。

## 实现方式：两类

- **区间匹配** —— 只有 `AgeTag` 用。把三级标签的 rule 按 `-` 切成 `start`/`end`，与业务字段做 `between` 比较。
- **枚举查表** —— 其余 5 个（性别、职业、政治面貌、婚姻状况、国籍）统一走工具类 `tags/utils/three_tag_id_mapper.py` 的 `ThreeTagIdMapper`：

  ```python
  get_tagid = ThreeTagIdMapper.to_udf(three_tag_df, value_map=..., default_rule=...)
  ```

  它把"三级标签配置 → `{rule: id}` 字典 → 查表 UDF"三步封装成静态方法，**调用时直接用类名，不要实例化**（utils 编码约定见根 `CLAUDE.md`）。

## 各标签的转换要点（实测自 `data/mysql/hive_data.sql` 全量 21208 条）

| 标签 | 源字段实际取值 | 需值转换？ | 空值/异常值归口 |
|---|---|---|---|
| 性别 | `1`/`2`/`0`，另有 **2 条脏值 `3`** | 否 | `default_rule='0'` → id=7「未知」 |
| 年龄段 | 生日 `yyyy-MM-dd` | 否（做格式处理） | — |
| 职业 | `1`~`6` + **NULL 8245 条（39%）** | 否 | `default_rule='7'` → id=84「其他」 |
| 婚姻状况 | `1`/`2`/`3` + **NULL 3936 条（19%）** | 否 | `default_rule='1'` → id=68「未婚」 |
| 政治面貌 | `1`团员 / `2`党员 / `3`群众 / `4`其他党派 | **是** | `default_rule='1'` → 群众 |
| 国籍 | **中文**：中国大陆 / 中国香港 / 中国澳门 / 中国台湾 / 其他 | **是** | `default_rule='5'` → 其他 |

> ⚠️ **政治面貌是唯一「编号含义冲突」的标签**：源表 `3` 是"群众"，而三级标签 rule `3` 是"其他党派"。直接把字段值当 rule 用会把这两类人打反，必须走 `POLITICS_TO_RULE = {'1':'1', '3':'1', '2':'2', '4':'3'}`。
>
> ⚠️ **国籍的字段是中文、rule 是数字**，必须走 `ADDRESS_TO_RULE` 转换，否则一条都匹配不上。

**6 个标签的实现方式已全部统一**：`AgeTag` 走 `ThreeTagIdJoiner.by_range` 做区间匹配，其余 5 个走 `ThreeTagIdMapper.to_udf` 做枚举查表 —— 彼此之间只差一个 `default_rule`（性别 `'0'`、职业 `'7'`、政治面貌 `'1'`、婚姻状况 `'1'`、国籍 `'5'`）。新增标签照这个模式写即可。

所有类底部都有 `if __name__ == "__main__"`，`two_tag_id` 是**硬编码**的（4/15/8/63/67/71），不是从参数读。生产调度时由 DolphinScheduler 提交。
