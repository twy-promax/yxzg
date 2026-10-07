-- 大屏 B 组件 B03 · 浏览器占比（环形图）
-- 来源：log_analysis_db.nginx_log_result；browser_name 是简称

SELECT
    COALESCE(NULLIF(browser_name, ''), '未知') AS 浏览器,
    COUNT(*)                                   AS 独立IP数
FROM log_analysis_db.nginx_log_result
GROUP BY COALESCE(NULLIF(browser_name, ''), '未知')
ORDER BY 独立IP数 DESC;
