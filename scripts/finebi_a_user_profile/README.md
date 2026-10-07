# 大屏 A · 用户画像 —— FineBI SQL 数据集

本目录放大屏 A 的 **21 个 SQL 数据集**：**一个文件 = FineBI 里一个 SQL 数据集 = 大屏上一个组件**。
用法：在 FineBI 里新建「SQL 数据集」→ 打开对应文件 → 全文复制 → 粘贴。

> ⭐ **首选走视图路线（更省事、更快、更不容易出错）**：先执行
> [`../doris_scripts/finebi_dashboard_views.sql`](../doris_scripts/finebi_dashboard_views.sql)
> 建好内表 `user_tag_long` 和 18 个视图，之后 FineBI 里只需「选表 → 拖字段」，**一个 SQL 都不用写**。
> 逐步操作见同目录 [`操作清单.md`](操作清单.md)。
> 本目录这 21 个 SQL 文件作为**备选/对照**保留（不想动 Doris、或想临时验证某个口径时用）。

数据来源是 Doris 的 **两个 catalog**：ES catalog（读标签结果）+ MySQL catalog（读标签字典），
所以**不需要新建任何 ETL 作业或宽表**，分布全部现算。

---

## 一、用之前必做两步

### 1. 替换两个表名（每个文件都要改，全文替换）

| 占位名 | 含义 |
|---|---|
| `es_cat.default_db.user_profile_tags` | 你的 **ES catalog** 下标签索引的真实全名 |
| `mysql_cat.tags_info.tbl_basic_tag` | 你的 **MySQL catalog** 下字典表的真实全名 |

不确定名字就执行：

```sql
SHOW CATALOGS;
SWITCH es_cat;     SHOW DATABASES; SHOW TABLES;
SWITCH mysql_cat;  SHOW DATABASES; SHOW TABLES;
```

> 已知：字典表 `tbl_basic_tag` 的列是 `id / name / rule / level / pid`，level=2 是标签类别（17 个），level=3 是标签值（82 个）。

### 2. 验证 ES 取出来的是「原始逗号串」

索引里 `tags_id_times` 是 **text + comma 分词器**，要确认 Doris 读的是 `_source` 原文：

```sql
SELECT user_id, tags_id_times, tags_id_once, tags_id_streaming
FROM es_cat.default_db.user_profile_tags LIMIT 5;
```

期望：`5,16,9,64,68,72,25,32,48,53,59,79,40` 这种形态。
**如果是被切碎的单个词或数组，先别往下做**，需要调 catalog 属性（`enable_docvalue_scan` / `enable_keyword_sniff`）。

再验证拆分函数可用：

```sql
SELECT tag_id FROM es_cat.default_db.user_profile_tags
LATERAL VIEW explode_split('5,16,9', ',') tmp AS tag_id;
-- 期望 3 行；若报错，把所有 SQL 里的 explode_split(字段, ',') 换成 explode(split_by_string(字段, ','))
```

---

## 二、组件清单（文件名 ↔ 大屏位置 ↔ 标签）

| 文件 | 组件 | 图表类型 | 2 级标签 id (pid) | ES 字段 |
|---|---|---|---|---|
| `KPI1_用户总数.sql` | KPI 用户总数 | 指标卡 | — | — |
| `KPI2_已打标用户数.sql` | KPI 已打标用户数 | 指标卡 | — | — |
| `KPI3_标签值总数.sql` | KPI 标签值总数（82） | 指标卡 | — | — |
| `KPI4_标签类别数.sql` | KPI 标签类别数（17） | 指标卡 | — | — |
| `A01_性别.sql` | 性别 | 环形图 | 4 | tags_id_times |
| `A02_年龄段.sql` | 年龄段 | 柱状图 | 15 | tags_id_times |
| `A03_职业.sql` | 职业 | 横向条形图 | 8 | tags_id_times |
| `A04_婚姻状况.sql` | 婚姻状况 | 环形图 | 67 | tags_id_times |
| `A05_政治面貌.sql` | 政治面貌 | 环形图 | 63 | tags_id_times |
| `A06_国籍.sql` | 国籍 | 横向条形图 | 71 | tags_id_times |
| `A07_客户价值.sql` | **客户价值（主视觉）** | 玫瑰图 / 柱状图 | 39 | tags_id_times |
| `A08_RFM.sql` | RFM 用户分层 | 柱状图 / 热力 | 113 | **tags_id_once** |
| `A09_客单价.sql` | 客单价 | 柱状图 | 78 | tags_id_times |
| `A10_支付方式.sql` | 支付方式 | 环形图 | 31 | tags_id_times |
| `A11_消费周期.sql` | 消费周期 | 横向条形图 | 24 | tags_id_times |
| `A12_活跃度.sql` | 活跃度 | 柱状图 | 47 | tags_id_times |
| `A13_PSM价格敏感度.sql` | 价格敏感度 PSM | 横向条形图 | 52 | tags_id_times |
| `A14_新老会员.sql` | 新老会员 | 环形图 | 85 | **tags_id_once** |
| `A15_购物性别USG.sql` | 购物性别 USG（建议不上屏） | 环形图 | 58 | tags_id_times |
| `A16_转化率.sql` | 转化率（实时） | 环形图 | 123 | **tags_id_streaming** |
| `A17_近期活跃度.sql` | 近期活跃度（实时） | 柱状图 | 127 | **tags_id_streaming** |

**字段分配规则**（决定一个标签在哪个字段里）：

| 字段 | 装哪些标签 | 谁写的 |
|---|---|---|
| `tags_id_times` | 匹配类 6 + 统计类 DSL 6 + 客户价值 | `TagBase` 流水线 |
| `tags_id_once` | 新老会员(85) + RFM(113) | SQL 系经 SeaTunnel 单独写 |
| `tags_id_streaming` | 转化率(123) + 近期活跃度(127) | 实时标签单独写 |

> 一个字段里混着多个标签没关系 —— 3 级标签 id 全局唯一，按 `d.pid` 过滤就能把每个类别分开。

---

## 三、大屏布局（1920×1080 固定，深色主题）

```
┌──────────────────────────────────────────────────────────────────────┐
│  云鲜智购 · 用户画像总览       标签 17 类 / 82 值     数据截止 xxxx    │
├──────────────────────────────────────────────────────────────────────┤
│ KPI1 用户总数 │ KPI2 已打标 │ KPI3 标签值 82 │ KPI4 标签类别 17       │
├──────────────┬──────────────────────────────────┬────────────────────┤
│ A01 性别 环形 │ A07 客户价值 玫瑰图（最大）       │ A11 消费周期 横向条│
│ A02 年龄段 柱 │ A08 RFM 分层 柱                   │ A12 活跃度 柱      │
│ A03 职业 条   │ A09 客单价 柱                     │ A14 新老会员 环形  │
│ A04 婚姻 环形 │                                   │ A13 PSM 条         │
│ A05 政治 环形 │                                   │ A16 转化率 环形    │
│ A06 国籍 条   │                                   │ A17 近期活跃度 柱  │
├──────────────┴──────────────────────────────────┴────────────────────┤
│ A10 支付方式 环形 │ A15 USG 环形（可选）                                │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 四、四个必须注意的点

1. **`TRIM(d.name)` 不能删**。字典里 `重要挽留用户 ` 和 `一般发展用户 ` **带尾随空格**，不 TRIM 会出现"看着一样但是两个维度"的脏项。
   *更好的做法*：直接在 MySQL 里把这两个名字的空格 UPDATE 掉，这样以后所有查询都不用 TRIM。
2. **排序用「排序」列，不要用「标签值」排**。年龄段、消费周期、客单价、PSM 这些档位按字典序排会错乱（`10-15` 排到 `5-15` 前面）。每个数据集都带了「排序」列（= 3 级标签 id，本身就是业务顺序），在 FineBI 里按它升序。
3. **如果 FineBI 报 `ORDER BY` 语法错**，删掉 SQL 最后一行 `ORDER BY d.id;` —— 结果完全一样，只是改由 FineBI 按「排序」列排。
4. **每个图的分母不一样**：这些 SQL 是 `INNER JOIN` 字典 + 只统计"有该标签"的用户。所以环形图的百分比是"占**有该标签的**用户"的比例，不是占全体用户。
   若要改成"占全体用户"（没打标的补 0），需要把每个 SQL 改成 `LEFT JOIN` 补默认档 —— 需要的话我可以再出一版。

---

## 五、性能与降级方案

每个数据集都会扫全表 + explode（约 2.1 万用户 × 19 个 id ≈ 40 万行中间结果）。单次查询很快，但 **17 个组件 = 17 次**，如果打开大屏卡顿，就在 Doris 里落一张内表，只拆一次：

```sql
CREATE TABLE internal.recommend_db.user_tag_long AS
SELECT t.user_id, TRIM(d.name) AS 标签值, d.id AS 标签id, d.pid AS 标签类别id
FROM (
    SELECT user_id, tag_id
    FROM es_cat.default_db.user_profile_tags
    LATERAL VIEW explode_split(tags_id_times, ',') tmp AS tag_id
    WHERE tags_id_times IS NOT NULL AND tags_id_times <> ''
) t
JOIN mysql_cat.tags_info.tbl_basic_tag d ON CAST(d.id AS STRING) = TRIM(t.tag_id);
```

之后每个组件的 SQL 就退化成：

```sql
SELECT 标签值, 标签id AS 排序, COUNT(DISTINCT user_id) AS 用户数
FROM recommend_db.user_tag_long
WHERE 标签类别id = 4
GROUP BY 标签值, 标签id ORDER BY 标签id;
```

**建议：先用本目录的现算版本，卡了再落地。**

---

## 六、上屏前的数据预期（避免误判成 bug）

- **A12 活跃度**：实测约 **94%** 用户是「非常不活跃」—— 文档档位是按 90 天活跃度设计的，样本撑不起来。
- **A15 USG 购物性别**：实测约 **99%** 是「中性」—— USG 依赖的 10 个特征编码（8 品类 + 2 商品）在现有数据源里**完全不存在**，是数据源问题。建议不上屏。
- **A13 PSM**：有两条实现路线并存（统计类 `psm_tag.py` + 挖掘类 `psm_ml.py`），都写同一批 id 53–57，**最终值取决于哪条后跑**。
- **A16/A17 实时**：覆盖用户数远少于离线标签（只有被实时任务算到的用户才有值），建议标注「实时」并单独设刷新频率。

---

## 七、报错排查记录：`'' is not a number`（FineBI 错误代码 62400001）

**报错原文**：

```
RuntimeException: java.util.concurrent.ExecutionException:
com.finebi.common.exception.conf.table.FineSqlErrorException:
错误代码:62400001  errCode = 2, detailMessage = '' is not a number
```

**原因**：`explode_split` 从 `tags_id_times` 里拆出来的 token 中含有**空串** `''`，
而最初的 JOIN 写的是 `ON d.id = CAST(t.tag_id AS BIGINT)`。
`CAST` 位于 **JOIN 的 `ON` 里，求值顺序早于 `WHERE`** —— 所以外层那个 `AND t.tag_id <> ''` 根本来不及拦。
结果是：只要有**一个**空 token，整个查询直接失败（不是少几行，是整块图出不来）。

**已修复**：全部 17 个组件改成**两边按字符串比较**，不再对 ES 里的 token 做数字转换：

```sql
JOIN mysql_cat.tags_info.tbl_basic_tag d
  ON CAST(d.id AS STRING) = TRIM(t.tag_id)
```

这样空 token / 脏值只会关联不上字典、被自然丢弃，**不会让查询崩**。

> 若你的 Doris 版本对 `CAST(... AS STRING)` 报错，等价写法是 `CONCAT(d.id, '') = TRIM(t.tag_id)`。

**建议顺手查清空 token 有多少、从哪来**（这两条都不做数字转换，不会报错）：

```sql
-- ① 空 token / 非数字 token 的规模
SELECT
    SUM(CASE WHEN TRIM(tag_id) = '' THEN 1 ELSE 0 END)                                  AS 空token,
    SUM(CASE WHEN TRIM(tag_id) <> '' AND tag_id NOT RLIKE '^[0-9]+$' THEN 1 ELSE 0 END) AS 非数字token,
    COUNT(*)                                                                            AS 总token数
FROM (
    SELECT tag_id
    FROM es_cat.default_db.user_profile_tags
    LATERAL VIEW explode_split(tags_id_times, ',') tmp AS tag_id
) t;

-- ② 挑出脏数据的用户
SELECT user_id, tags_id_times
FROM es_cat.default_db.user_profile_tags
WHERE tags_id_times LIKE '%,'  OR tags_id_times LIKE ',%'
   OR tags_id_times LIKE '%,,%' OR tags_id_times = ''
LIMIT 20;
```

**怎么解读**：

- 空 token 是 **0 或个位数** → 就是个别人的脏数据，用现在的 SQL 直接忽略即可，不用管。
- 空 token **数量不小** → 说明上游写入端产生过空串。已知可能路径：`TagBase.merge_tag` 的合并 UDF 在
  「新旧标签都为空」时会返回**空字符串**而不是 NULL，然后被写进 ES。这种要么在写入端改成写 NULL，
  要么在读取端保持现在这版 SQL（字符串比较 + 可选的 `RLIKE` 过滤）。
- 另外提醒：`tags_id_once`（新老会员 / RFM）和 `tags_id_streaming`（转化率 / 近期活跃度）
  走的是同一套模板，已一并修好；如果那两个字段里的脏值更多，同样的现象会出现在 A08 / A14 / A16 / A17。
