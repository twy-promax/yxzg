-- 大屏 B 组件 B02 · 设备系统占比（环形图）
-- 来源：log_analysis_db.nginx_log_result；空值统一归到「未知」

SELECT
    COALESCE(NULLIF(device_os, ''), '未知') AS 设备系统,
    COUNT(*)                                AS 独立IP数
FROM log_analysis_db.nginx_log_result
GROUP BY COALESCE(NULLIF(device_os, ''), '未知')
ORDER BY 独立IP数 DESC;
