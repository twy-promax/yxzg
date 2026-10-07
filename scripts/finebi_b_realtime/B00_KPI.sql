-- 大屏 B · KPI 条（一行六列，可在 FineBI 里拆成 6 张指标卡）
--
-- ⚠️ 口径必读：下面这三张 Doris 表都是 UNIQUE 模型、每实体只保留一行，
--    新数据会覆盖旧数据（不是历史追加的事实表）。所以这些数字是
--    「当前表内各实体最新值之和」，不是历史累计总量，解读时要说清楚。
--    也正因为如此，这些表做不了时间趋势图。

SELECT
    n.ip_cnt     AS 独立IP数,
    u.browse     AS 总浏览量,
    u.cart       AS 加购数,
    u.order_cnt  AS 下单数,
    u.buy        AS 支付数,
    u.back_order AS 退货数
FROM (
    SELECT COUNT(*) AS ip_cnt
    FROM log_analysis_db.nginx_log_result
) n
CROSS JOIN (
    SELECT SUM(browse_num)     AS browse,
           SUM(cart_num)       AS cart,
           SUM(order_num)      AS order_cnt,
           SUM(buy_num)        AS buy,
           SUM(back_order_num) AS back_order
    FROM log_analysis_db.user_event_result
) u;
