-- 大屏 B 组件 B09 · 关键词搜索数 TOP10（柱状图）

SELECT
    COALESCE(NULLIF(user_name, ''), CONCAT('用户', CAST(user_id AS STRING))) AS 用户,
    keywords_num AS 关键词数
FROM log_analysis_db.user_event_result
ORDER BY 关键词数 DESC
LIMIT 10;
