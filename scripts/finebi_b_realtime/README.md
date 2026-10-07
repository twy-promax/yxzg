# 大屏 B · 实时运营监控 —— FineBI SQL 数据集

本目录放大屏 B 的 **12 个 SQL 数据集**：**一个文件 = FineBI 里一个 SQL 数据集 = 大屏上一个组件**。
全部查 Doris **内部表**（`log_analysis_db` / `db_analysis_db` / `recommend_db`），**不需要任何 catalog**，直接复制粘贴即可用。

> ⭐ **首选走视图路线**：先执行
> [`../doris_scripts/finebi_dashboard_views.sql`](../doris_scripts/finebi_dashboard_views.sql)
> 的第四节建好 12 个 `v_b**` 视图，之后 FineBI 里只需「选表 → 拖字段」。
> 逐步操作与刷新频率设置见同目录 [`操作清单.md`](操作清单.md)。
> 本目录这 12 个 SQL 文件作为**备选/对照**保留。

---

## ⚠️ 一、口径必读：这三张表是「快照」，不是「事实表」

| 表 | 主键 | 真实语义 |
|---|---|---|
| `log_analysis_db.nginx_log_result` | `UNIQUE(ip)` | 一个 IP 一行；`pv` 等指标是该 IP **最新一批次**的聚合值，新数据覆盖旧数据 |
| `log_analysis_db.user_event_result` | `UNIQUE(user_id)` | 一个用户一行；各行为列是该用户**最新一次**聚合的值 |
| `db_analysis_db.shop_order_analysis` | `UNIQUE(user_id)` | 一个用户只有**最新那个 12 小时窗口**，跨窗口会被覆盖 |

**由此得出三条硬约束**：

1. 大屏上的数字口径是「**当前表内各实体最新值之和**」，不是历史累计总量 —— 图表上要写清楚，否则会被误读成"累计 PV 才这么点？"。
2. **这三张表做不了时间趋势图**（没有按时间追加的事实数据）。想要趋势，需要另建一张 `DUPLICATE` 模型的小时/分钟明细表。
3. `db_analysis_db.shop_order_analysis` **本目录故意不用它**：按用户去重后，`SUM(order_num)` 会严重低估"12 小时总下单量"。如果要用，只能做**用户维度明细**，不能做加总/趋势。

---

## 二、组件清单

| 文件 | 组件 | 图表类型 | 数据来源 |
|---|---|---|---|
| `B00_KPI.sql` | KPI 条（独立IP数/总浏览量/加购/下单/支付/退货，一行六列） | 指标卡 ×6 | nginx_log_result + user_event_result |
| `B01_状态码分布.sql` | HTTP 状态码分布 | 环形图 | nginx_log_result |
| `B02_设备系统占比.sql` | 设备系统 | 环形图 | nginx_log_result |
| `B03_浏览器占比.sql` | 浏览器 | 环形图 | nginx_log_result |
| `B04_设备品牌TOP10.sql` | 设备品牌 | 横向条形图 | nginx_log_result |
| `B05_行为转化漏斗.sql` | 浏览→加购→下单→支付→退单 | 漏斗图 | user_event_result |
| `B06_地区TOP10.sql` | 地区访问量 | 横向条形图 | nginx_log_result |
| `B07_停留时长TOP10.sql` | 用户停留时长 | 横向条形图 | user_event_result |
| `B08_价格与停留散点.sql` | 平均商品价格 × 停留时长 | 散点 / 气泡图 | user_event_result |
| `B09_关键词数TOP10.sql` | 关键词搜索数 | 柱状图 | user_event_result |
| `B10_热门商品TOP10.sql` | 热门商品榜 | 表格 / 条形图 | recommend_db.popular_hot_goods |
| `B11_关联规则TOP10.sql` | 关联规则榜 | 表格 | recommend_db.fpgrowth_association_goods |

---

## 三、大屏布局（1920×1080 固定，深色主题）

```
┌──────────────────────────────────────────────────────────────────────┐
│  云鲜智购 · 实时运营监控            数据源：Doris    每 60 秒刷新      │
├──────────────────────────────────────────────────────────────────────┤
│ B00：独立IP数 │ 总浏览量 │ 加购数 │ 下单数 │ 支付数 │ 退货数           │
├──────────────────┬────────────────────────────┬──────────────────────┤
│ B01 状态码 环形   │ B05 用户行为转化漏斗        │ B06 地区 TOP10 条     │
│ B02 设备系统 环形 │   （最大的一块）            │ B07 停留时长 TOP10    │
│ B03 浏览器 环形   │                            │ B08 价格×停留 散点    │
│ B04 设备品牌 条   │                            │ B09 关键词数 柱       │
├──────────────────┴────────────────────────────┴──────────────────────┤
│ B10 热门商品 TOP10（表格）      │ B11 关联规则 TOP10（表格）           │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 四、前置条件与刷新策略

**前置条件**：`B10` / `B11` 依赖两个推荐作业已经跑过，否则表格是空的：

```bash
# 热门商品 → recommend_db.popular_hot_goods
spark-submit --master 'local[*]' --py-files tags.zip tags/recommend/popular_goods_hot.py
# 关联规则 + 模型落盘 → recommend_db.fpgrowth_association_goods
spark-submit --master 'local[*]' --py-files tags.zip tags/recommend/fpgrowth_association_goods.py
```

**刷新频率**（FineBI 里按组件分别设）：

| 组件 | 建议 |
|---|---|
| B00–B09（实时链路产出） | 30 ~ 60 秒 |
| B10 / B11（离线批产出，一天一次） | **1 天一次**，或跟随作业完成时间刷新 |

> 别给 B10/B11 设成实时刷新 —— 数据根本不变，只会白耗查询。

---

## 五、两个可能报错的点

1. **`B11` 的 `ARRAY_JOIN`**：`antecedent`/`consequent` 是 `ARRAY<STRING>`。若你的 Doris 版本没有 `ARRAY_JOIN`，把它换成 `CAST(antecedent AS STRING)`。
2. **`B10` / `B11` 的最新批次过滤**：两张表都是动态分区表，SQL 里用 `WHERE 日期列 = (SELECT MAX(日期列) ...)` 只取最新一次计算。**这个条件不能删** —— 删了会把历史所有分区的数据加总，商品榜单会虚高好几倍。

---

## 六、和标签大屏的关系

大屏 A（`../finebi_a_user_profile/`）是**离线标签画像**，数据来自 ES 的标签结果，一天更新一次；
大屏 B 是**实时运营指标**，数据来自实时数仓写进 Doris 的结果，秒级/分钟级更新。

两块屏**数据源完全独立**，可以分开做、分开刷新，互不影响。建议先做 B（零改造、当天能出效果），再做 A。
