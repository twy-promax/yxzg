-- 大屏 B 组件 B08 · 平均商品价格 × 停留时长（散点图 / 气泡图）
-- 来源：log_analysis_db.user_event_result
-- 用法：X 轴放「平均商品价格」，Y 轴放「停留分钟」，一个点一个用户

SELECT
    COALESCE(NULLIF(user_name, ''), CONCAT('用户', CAST(user_id AS STRING))) AS 用户,
    ROUND(avg_price, 2)      AS 平均商品价格,
    ROUND(stay_duration, 2)  AS 停留分钟,
    browse_num               AS 浏览行为数
FROM log_analysis_db.user_event_result
WHERE avg_price IS NOT NULL AND stay_duration IS NOT NULL
ORDER BY 平均商品价格 DESC
LIMIT 200;
