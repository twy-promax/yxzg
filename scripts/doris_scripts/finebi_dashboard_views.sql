-- ============================================================================
-- FineBI 大屏专用：Doris 内表 + 视图
-- ============================================================================
-- 目的：把「拆开标签 id 串 + 翻译成中文」这件重活**只做一次**，落到一张内表里，
--       之后每个大屏组件都查视图 —— FineBI 侧只需「选表 → 拖字段」，不用写 SQL。
--
-- 收益：① 不再每次全表 explode（17 个组件从 17 次扫描降到 1 次落地）
--       ② SQL 里不再出现任何 CAST，彻底避免 '' is not a number 那类报错
--       ③ 口径统一固化在 Doris 侧，FineBI 里改不动
--
-- 执行方式：mysql -h up01 -P 9030 -uroot -p < finebi_dashboard_views.sql
--          （或在任意 Doris 客户端里整段执行；脚本可重复执行，视图会先 DROP 再建）
--
-- ⚠️ catalog 名以你当前环境为准。本脚本用的是从你 FineBI 截图里读到的真实名字：
--      ES catalog     = es          （索引 es.default_db.user_profile_tags）
--      MySQL catalog  = jdbc_mysql  （字典 jdbc_mysql.tags_info.tbl_basic_tag）
--    若以后改了 catalog 名，只要改下面「一、二」两节里的这两处即可（视图依赖内表，不用改）。
-- ============================================================================


-- ============================================================================
-- 一、大屏 A 的两张内表（建一次就够）
-- ============================================================================

-- 1.1 用户标签长表：一行 = 一个用户的一个标签
CREATE TABLE IF NOT EXISTS recommend_db.user_tag_long (
    user_id         BIGINT       COMMENT '用户id',
    source_field    VARCHAR(30)  COMMENT '来源字段：tags_id_times / tags_id_once / tags_id_streaming',
    tag_category_id BIGINT       COMMENT '2级标签id（标签类别，如 4=性别）',
    tag_id          BIGINT       COMMENT '3级标签id（标签值，如 5=男）',
    tag_value       VARCHAR(50)  COMMENT '3级标签名称（已 TRIM，中文）',
    refresh_time    DATETIME     COMMENT '本次刷新时间'
)
DUPLICATE KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 10
PROPERTIES ("replication_num" = "1");

-- 1.2 大屏 KPI 表：只有一行，四个指标
CREATE TABLE IF NOT EXISTS recommend_db.user_tag_stat (
    refresh_time     DATETIME COMMENT '本次刷新时间',
    total_users      BIGINT   COMMENT '用户总数',
    tagged_users     BIGINT   COMMENT '已打标用户数',
    tag_value_cnt    BIGINT   COMMENT '标签值总数（level=3）',
    tag_category_cnt BIGINT   COMMENT '标签类别数（level=2）'
)
DUPLICATE KEY(refresh_time)
DISTRIBUTED BY HASH(refresh_time) BUCKETS 1
PROPERTIES ("replication_num" = "1");


-- ============================================================================
-- 二、刷新语句（★ 每次标签跑批完成后执行这一段；两条都要跑）
--
--     注意 1：这里是**跨 catalog 查询**（ES 的索引 + MySQL 的字典），已验证可行。
--
--     注意 2：**不能用 INSERT OVERWRITE** —— 本项目 Doris 1.2.1 的 INSERT OVERWRITE
--             只支持分区表，而下面这两张都是非分区表，实测会直接报语法错误。
--             所以统一改成「TRUNCATE TABLE 清空 + INSERT INTO 重写」，效果完全等价。
-- ============================================================================

TRUNCATE TABLE recommend_db.user_tag_long;
INSERT INTO recommend_db.user_tag_long
SELECT x.user_id,
       'tags_id_times'  AS source_field,
       d.pid            AS tag_category_id,
       d.id             AS tag_id,
       TRIM(d.name)     AS tag_value,
       NOW()            AS refresh_time
FROM (
    SELECT user_id, TRIM(tag_id) AS tag_id
    FROM es.default_db.user_profile_tags
    LATERAL VIEW explode_split(tags_id_times, ',') tmp AS tag_id
) x
JOIN jdbc_mysql.tags_info.tbl_basic_tag d ON CAST(d.id AS STRING) = x.tag_id
WHERE x.tag_id RLIKE '^[0-9]+$'
UNION ALL
SELECT x.user_id, 'tags_id_once', d.pid, d.id, TRIM(d.name), NOW()
FROM (
    SELECT user_id, TRIM(tag_id) AS tag_id
    FROM es.default_db.user_profile_tags
    LATERAL VIEW explode_split(tags_id_once, ',') tmp AS tag_id
) x
JOIN jdbc_mysql.tags_info.tbl_basic_tag d ON CAST(d.id AS STRING) = x.tag_id
WHERE x.tag_id RLIKE '^[0-9]+$'
UNION ALL
SELECT x.user_id, 'tags_id_streaming', d.pid, d.id, TRIM(d.name), NOW()
FROM (
    SELECT user_id, TRIM(tag_id) AS tag_id
    FROM es.default_db.user_profile_tags
    LATERAL VIEW explode_split(tags_id_streaming, ',') tmp AS tag_id
) x
JOIN jdbc_mysql.tags_info.tbl_basic_tag d ON CAST(d.id AS STRING) = x.tag_id
WHERE x.tag_id RLIKE '^[0-9]+$';

-- KPI 表：用两个单行聚合 CROSS JOIN，避免标量子查询的兼容性问题
TRUNCATE TABLE recommend_db.user_tag_stat;
INSERT INTO recommend_db.user_tag_stat
SELECT NOW() AS refresh_time,
       u.total_users,
       u.tagged_users,
       s.tag_value_cnt,
       s.tag_category_cnt
FROM (
    SELECT COUNT(DISTINCT user_id) AS total_users,
           COUNT(DISTINCT CASE WHEN COALESCE(tags_id_times, '') <> ''
                                 OR COALESCE(tags_id_once, '') <> ''
                                 OR COALESCE(tags_id_streaming, '') <> ''
                               THEN user_id END) AS tagged_users
    FROM es.default_db.user_profile_tags
) u
CROSS JOIN (
    SELECT SUM(CASE WHEN level = 3 THEN 1 ELSE 0 END) AS tag_value_cnt,
           SUM(CASE WHEN level = 2 THEN 1 ELSE 0 END) AS tag_category_cnt
    FROM jdbc_mysql.tags_info.tbl_basic_tag
) s;


-- ============================================================================
-- 三、大屏 A 的 18 个视图（17 个标签分布 + 1 个 KPI）
--     统一输出三列：标签值 / 排序 / 用户数。「排序」= 3 级标签 id，就是业务顺序。
--     在 FineBI 里把「标签值」当维度、「用户数」当度量、按「排序」升序。
-- ============================================================================

DROP VIEW IF EXISTS recommend_db.v_a00_kpi;
DROP VIEW IF EXISTS recommend_db.v_a01_sex;
DROP VIEW IF EXISTS recommend_db.v_a02_age_range;
DROP VIEW IF EXISTS recommend_db.v_a03_job;
DROP VIEW IF EXISTS recommend_db.v_a04_marital;
DROP VIEW IF EXISTS recommend_db.v_a05_politics;
DROP VIEW IF EXISTS recommend_db.v_a06_nation;
DROP VIEW IF EXISTS recommend_db.v_a07_customer_value;
DROP VIEW IF EXISTS recommend_db.v_a08_rfm;
DROP VIEW IF EXISTS recommend_db.v_a09_unit_price;
DROP VIEW IF EXISTS recommend_db.v_a10_pay_type;
DROP VIEW IF EXISTS recommend_db.v_a11_consumer_cycle;
DROP VIEW IF EXISTS recommend_db.v_a12_activity;
DROP VIEW IF EXISTS recommend_db.v_a13_psm;
DROP VIEW IF EXISTS recommend_db.v_a14_new_old_member;
DROP VIEW IF EXISTS recommend_db.v_a15_usg;
DROP VIEW IF EXISTS recommend_db.v_a16_conversion_rate;
DROP VIEW IF EXISTS recommend_db.v_a17_recent_activity;

-- A00 KPI：四个指标在一行里，FineBI 里做 4 张指标卡，每张取一列
CREATE VIEW recommend_db.v_a00_kpi AS
SELECT total_users      AS 用户总数,
       tagged_users     AS 已打标用户数,
       tag_value_cnt    AS 标签值总数,
       tag_category_cnt AS 标签类别数,
       refresh_time     AS 数据更新时间
FROM recommend_db.user_tag_stat;

-- A01 性别（环形图）—— 2 级标签 id = 4
CREATE VIEW recommend_db.v_a01_sex AS
SELECT tag_value AS 标签值, tag_id AS 排序, COUNT(DISTINCT user_id) AS 用户数
FROM recommend_db.user_tag_long WHERE tag_category_id = 4 GROUP BY tag_value, tag_id;

-- A02 年龄段（柱状图）—— 15
CREATE VIEW recommend_db.v_a02_age_range AS
SELECT tag_value AS 标签值, tag_id AS 排序, COUNT(DISTINCT user_id) AS 用户数
FROM recommend_db.user_tag_long WHERE tag_category_id = 15 GROUP BY tag_value, tag_id;

-- A03 职业（横向条形图）—— 8
CREATE VIEW recommend_db.v_a03_job AS
SELECT tag_value AS 标签值, tag_id AS 排序, COUNT(DISTINCT user_id) AS 用户数
FROM recommend_db.user_tag_long WHERE tag_category_id = 8 GROUP BY tag_value, tag_id;

-- A04 婚姻状况（环形图）—— 67
CREATE VIEW recommend_db.v_a04_marital AS
SELECT tag_value AS 标签值, tag_id AS 排序, COUNT(DISTINCT user_id) AS 用户数
FROM recommend_db.user_tag_long WHERE tag_category_id = 67 GROUP BY tag_value, tag_id;

-- A05 政治面貌（环形图）—— 63
CREATE VIEW recommend_db.v_a05_politics AS
SELECT tag_value AS 标签值, tag_id AS 排序, COUNT(DISTINCT user_id) AS 用户数
FROM recommend_db.user_tag_long WHERE tag_category_id = 63 GROUP BY tag_value, tag_id;

-- A06 国籍（横向条形图）—— 71
CREATE VIEW recommend_db.v_a06_nation AS
SELECT tag_value AS 标签值, tag_id AS 排序, COUNT(DISTINCT user_id) AS 用户数
FROM recommend_db.user_tag_long WHERE tag_category_id = 71 GROUP BY tag_value, tag_id;

-- A07 客户价值（玫瑰图，主视觉）—— 39
CREATE VIEW recommend_db.v_a07_customer_value AS
SELECT tag_value AS 标签值, tag_id AS 排序, COUNT(DISTINCT user_id) AS 用户数
FROM recommend_db.user_tag_long WHERE tag_category_id = 39 GROUP BY tag_value, tag_id;

-- A08 RFM 用户分层（柱状图）—— 113
CREATE VIEW recommend_db.v_a08_rfm AS
SELECT tag_value AS 标签值, tag_id AS 排序, COUNT(DISTINCT user_id) AS 用户数
FROM recommend_db.user_tag_long WHERE tag_category_id = 113 GROUP BY tag_value, tag_id;

-- A09 客单价（柱状图）—— 78
CREATE VIEW recommend_db.v_a09_unit_price AS
SELECT tag_value AS 标签值, tag_id AS 排序, COUNT(DISTINCT user_id) AS 用户数
FROM recommend_db.user_tag_long WHERE tag_category_id = 78 GROUP BY tag_value, tag_id;

-- A10 支付方式（环形图）—— 31
CREATE VIEW recommend_db.v_a10_pay_type AS
SELECT tag_value AS 标签值, tag_id AS 排序, COUNT(DISTINCT user_id) AS 用户数
FROM recommend_db.user_tag_long WHERE tag_category_id = 31 GROUP BY tag_value, tag_id;

-- A11 消费周期（横向条形图）—— 24
CREATE VIEW recommend_db.v_a11_consumer_cycle AS
SELECT tag_value AS 标签值, tag_id AS 排序, COUNT(DISTINCT user_id) AS 用户数
FROM recommend_db.user_tag_long WHERE tag_category_id = 24 GROUP BY tag_value, tag_id;

-- A12 活跃度（柱状图）—— 47
CREATE VIEW recommend_db.v_a12_activity AS
SELECT tag_value AS 标签值, tag_id AS 排序, COUNT(DISTINCT user_id) AS 用户数
FROM recommend_db.user_tag_long WHERE tag_category_id = 47 GROUP BY tag_value, tag_id;

-- A13 价格敏感度 PSM（横向条形图）—— 52
CREATE VIEW recommend_db.v_a13_psm AS
SELECT tag_value AS 标签值, tag_id AS 排序, COUNT(DISTINCT user_id) AS 用户数
FROM recommend_db.user_tag_long WHERE tag_category_id = 52 GROUP BY tag_value, tag_id;

-- A14 新老会员（环形图）—— 85
CREATE VIEW recommend_db.v_a14_new_old_member AS
SELECT tag_value AS 标签值, tag_id AS 排序, COUNT(DISTINCT user_id) AS 用户数
FROM recommend_db.user_tag_long WHERE tag_category_id = 85 GROUP BY tag_value, tag_id;

-- A15 购物性别 USG（环形图，实测约 99% 是「中性」，建议不上屏）—— 58
CREATE VIEW recommend_db.v_a15_usg AS
SELECT tag_value AS 标签值, tag_id AS 排序, COUNT(DISTINCT user_id) AS 用户数
FROM recommend_db.user_tag_long WHERE tag_category_id = 58 GROUP BY tag_value, tag_id;

-- A16 转化率（环形图，实时标签）—— 123
CREATE VIEW recommend_db.v_a16_conversion_rate AS
SELECT tag_value AS 标签值, tag_id AS 排序, COUNT(DISTINCT user_id) AS 用户数
FROM recommend_db.user_tag_long WHERE tag_category_id = 123 GROUP BY tag_value, tag_id;

-- A17 近期活跃度（柱状图，实时标签）—— 127
CREATE VIEW recommend_db.v_a17_recent_activity AS
SELECT tag_value AS 标签值, tag_id AS 排序, COUNT(DISTINCT user_id) AS 用户数
FROM recommend_db.user_tag_long WHERE tag_category_id = 127 GROUP BY tag_value, tag_id;


-- ============================================================================
-- 四、大屏 B 的 12 个视图（11 个实时组件 + 1 个 KPI）
--     全部只查 Doris 内部表，无 catalog 依赖。
--
--     ⚠️ 口径提醒：下面三张表是 UNIQUE 模型（每实体一行，新数据覆盖旧数据），
--        所以数字是「当前表内各实体最新值之和」，不是历史累计总量；
--        也正因为如此，这些视图做不了时间趋势图。
-- ============================================================================

DROP VIEW IF EXISTS recommend_db.v_b00_kpi;
DROP VIEW IF EXISTS recommend_db.v_b01_status_code;
DROP VIEW IF EXISTS recommend_db.v_b02_device_os;
DROP VIEW IF EXISTS recommend_db.v_b03_browser;
DROP VIEW IF EXISTS recommend_db.v_b04_device_brand;
DROP VIEW IF EXISTS recommend_db.v_b05_funnel;
DROP VIEW IF EXISTS recommend_db.v_b06_area;
DROP VIEW IF EXISTS recommend_db.v_b07_stay_duration;
DROP VIEW IF EXISTS recommend_db.v_b08_price_stay;
DROP VIEW IF EXISTS recommend_db.v_b09_keywords;
DROP VIEW IF EXISTS recommend_db.v_b10_hot_goods;
DROP VIEW IF EXISTS recommend_db.v_b11_association;

-- B00 KPI：一行六列，FineBI 里做 6 张指标卡
CREATE VIEW recommend_db.v_b00_kpi AS
SELECT n.ip_cnt     AS 独立IP数,
       u.browse     AS 总浏览量,
       u.cart       AS 加购数,
       u.order_cnt  AS 下单数,
       u.buy        AS 支付数,
       u.back_order AS 退货数
FROM (SELECT COUNT(*) AS ip_cnt FROM log_analysis_db.nginx_log_result) n
CROSS JOIN (
    SELECT SUM(browse_num)     AS browse,
           SUM(cart_num)       AS cart,
           SUM(order_num)      AS order_cnt,
           SUM(buy_num)        AS buy,
           SUM(back_order_num) AS back_order
    FROM log_analysis_db.user_event_result
) u;

-- B01 HTTP 状态码分布（环形图）
CREATE VIEW recommend_db.v_b01_status_code AS
SELECT status_code AS 状态码, SUM(pv) AS 访问量
FROM log_analysis_db.nginx_log_result
GROUP BY status_code;

-- B02 设备系统占比（环形图）
CREATE VIEW recommend_db.v_b02_device_os AS
SELECT COALESCE(NULLIF(device_os, ''), '未知') AS 设备系统, COUNT(*) AS 独立IP数
FROM log_analysis_db.nginx_log_result
GROUP BY COALESCE(NULLIF(device_os, ''), '未知');

-- B03 浏览器占比（环形图）
CREATE VIEW recommend_db.v_b03_browser AS
SELECT COALESCE(NULLIF(browser_name, ''), '未知') AS 浏览器, COUNT(*) AS 独立IP数
FROM log_analysis_db.nginx_log_result
GROUP BY COALESCE(NULLIF(browser_name, ''), '未知');

-- B04 设备品牌 TOP10（横向条形图）
CREATE VIEW recommend_db.v_b04_device_brand AS
SELECT COALESCE(NULLIF(device_brand, ''), '未知') AS 设备品牌, COUNT(*) AS 独立IP数
FROM log_analysis_db.nginx_log_result
GROUP BY COALESCE(NULLIF(device_brand, ''), '未知');

-- B05 用户行为转化漏斗（漏斗图，按「排序」升序）
CREATE VIEW recommend_db.v_b05_funnel AS
SELECT 1 AS 排序, '浏览' AS 行为阶段, SUM(browse_num)     AS 次数 FROM log_analysis_db.user_event_result
UNION ALL SELECT 2, '加购', SUM(cart_num)       FROM log_analysis_db.user_event_result
UNION ALL SELECT 3, '下单', SUM(order_num)      FROM log_analysis_db.user_event_result
UNION ALL SELECT 4, '支付', SUM(buy_num)        FROM log_analysis_db.user_event_result
UNION ALL SELECT 5, '退单', SUM(back_order_num) FROM log_analysis_db.user_event_result;

-- B06 地区访问 TOP10（横向条形图）
CREATE VIEW recommend_db.v_b06_area AS
SELECT COALESCE(NULLIF(area, ''), '未知') AS 地区, SUM(pv) AS 访问量, COUNT(*) AS 独立IP数
FROM log_analysis_db.nginx_log_result
GROUP BY COALESCE(NULLIF(area, ''), '未知');

-- B07 用户停留时长 TOP10（横向条形图）
CREATE VIEW recommend_db.v_b07_stay_duration AS
SELECT COALESCE(NULLIF(user_name, ''), CONCAT('用户', CAST(user_id AS STRING))) AS 用户,
       ROUND(stay_duration, 2) AS 停留分钟,
       browse_page_num         AS 浏览页面数
FROM log_analysis_db.user_event_result
WHERE stay_duration IS NOT NULL;

-- B08 平均商品价格 × 停留时长（散点图）
CREATE VIEW recommend_db.v_b08_price_stay AS
SELECT COALESCE(NULLIF(user_name, ''), CONCAT('用户', CAST(user_id AS STRING))) AS 用户,
       ROUND(avg_price, 2)     AS 平均商品价格,
       ROUND(stay_duration, 2) AS 停留分钟,
       browse_num              AS 浏览行为数
FROM log_analysis_db.user_event_result
WHERE avg_price IS NOT NULL AND stay_duration IS NOT NULL;

-- B09 关键词搜索数 TOP10（柱状图）
CREATE VIEW recommend_db.v_b09_keywords AS
SELECT COALESCE(NULLIF(user_name, ''), CONCAT('用户', CAST(user_id AS STRING))) AS 用户,
       keywords_num AS 关键词数
FROM log_analysis_db.user_event_result;

-- B10 热门商品 TOP10（表格）—— 只取最新分区的结果
CREATE VIEW recommend_db.v_b10_hot_goods AS
SELECT goods_no            AS 商品编码,
       goods_name          AS 商品名称,
       third_category_name AS 三级品类,
       goods_num           AS 销量
FROM recommend_db.popular_hot_goods
WHERE recommend_date = (SELECT MAX(recommend_date) FROM recommend_db.popular_hot_goods);

-- B11 关联规则 TOP10（表格）—— 只取最新一次计算
CREATE VIEW recommend_db.v_b11_association AS
SELECT ARRAY_JOIN(antecedent, ',') AS 前项商品,
       ARRAY_JOIN(consequent, ',') AS 后项商品,
       ROUND(confidence, 3)        AS 置信度,
       ROUND(lift, 2)              AS 提升度,
       ROUND(support, 4)           AS 支持度
FROM recommend_db.fpgrowth_association_goods
WHERE calculate_date = (SELECT MAX(calculate_date) FROM recommend_db.fpgrowth_association_goods);


-- ============================================================================
-- 五、自检（执行完上面全部内容后跑这几条，看到数据才算成功）
-- ============================================================================
-- 1) 长表有没有数据、覆盖了多少用户与标签类别
   SELECT COUNT(*) AS 行数, COUNT(DISTINCT user_id) AS 用户数, COUNT(DISTINCT tag_category_id) AS 类别数
   FROM recommend_db.user_tag_long;
--    期望：行数约 38~42 万，用户数 = 21208，类别数 = 17
--
# 2) KPI 对不对
   SELECT * FROM recommend_db.v_a00_kpi;
--    期望：用户总数 21208、标签值总数 82、标签类别数 17
--
-- 3) 性别分布（应与 FineBI 里那条 SQL 完全一致）
   SELECT * FROM recommend_db.v_a01_sex ORDER BY 排序;
--    期望：5=男 1845、6=女 4324、7=未知 15039
-- ============================================================================
