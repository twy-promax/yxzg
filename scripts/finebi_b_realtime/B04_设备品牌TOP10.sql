-- 大屏 B 组件 B04 · 设备品牌 TOP10（横向条形图）

SELECT
    COALESCE(NULLIF(device_brand, ''), '未知') AS 设备品牌,
    COUNT(*)                                   AS 独立IP数
FROM log_analysis_db.nginx_log_result
GROUP BY COALESCE(NULLIF(device_brand, ''), '未知')
ORDER BY 独立IP数 DESC
LIMIT 10;
