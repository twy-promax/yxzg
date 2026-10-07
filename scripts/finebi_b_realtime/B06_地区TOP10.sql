-- 大屏 B 组件 B06 · 地区访问 TOP10（横向条形图）
-- 来源：log_analysis_db.nginx_log_result；area 是按 IP 解析出来的地区

SELECT
    COALESCE(NULLIF(area, ''), '未知') AS 地区,
    SUM(pv)                            AS 访问量,
    COUNT(*)                           AS 独立IP数
FROM log_analysis_db.nginx_log_result
GROUP BY COALESCE(NULLIF(area, ''), '未知')
ORDER BY 访问量 DESC
LIMIT 10;
