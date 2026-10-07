-- 大屏 B 组件 B07 · 用户停留时长 TOP10（横向条形图）
-- 来源：log_analysis_db.user_event_result；stay_duration 单位是分钟

SELECT
    COALESCE(NULLIF(user_name, ''), CONCAT('用户', CAST(user_id AS STRING))) AS 用户,
    ROUND(stay_duration, 2)      AS 停留分钟,
    browse_page_num              AS 浏览页面数
FROM log_analysis_db.user_event_result
WHERE stay_duration IS NOT NULL
ORDER BY 停留分钟 DESC
LIMIT 10;
