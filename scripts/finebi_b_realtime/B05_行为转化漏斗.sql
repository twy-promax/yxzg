-- 大屏 B 组件 B05 · 用户行为转化漏斗（漏斗图）
-- 来源：log_analysis_db.user_event_result（一个用户一行，各列是该用户最新一次聚合的行为数）
-- 输出三列：排序 / 行为阶段 / 次数。漏斗图请按「排序」升序，别按次数排（会倒过来）
--
-- ⚠️ 各阶段之间不是严格的「同一批人逐级递减」：这些列是各自独立聚合的，
--    下单数 > 浏览数的情况理论上不该出现，但如果出现，说明上游窗口口径不一致，需要回查实时任务。

SELECT 1 AS 排序, '浏览' AS 行为阶段, SUM(browse_num)     AS 次数 FROM log_analysis_db.user_event_result
UNION ALL SELECT 2, '加购', SUM(cart_num)       FROM log_analysis_db.user_event_result
UNION ALL SELECT 3, '下单', SUM(order_num)      FROM log_analysis_db.user_event_result
UNION ALL SELECT 4, '支付', SUM(buy_num)        FROM log_analysis_db.user_event_result
UNION ALL SELECT 5, '退单', SUM(back_order_num) FROM log_analysis_db.user_event_result
ORDER BY 排序;
