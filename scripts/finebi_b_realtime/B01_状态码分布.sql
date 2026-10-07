-- 大屏 B 组件 B01 · HTTP 状态码分布（环形图）
-- 来源：log_analysis_db.nginx_log_result（一个 IP 一行，pv 是该 IP 最新一次聚合的值）
-- 度量取 sum(pv)，口径 = 当前表内各 IP 的 PV 之和

SELECT
    status_code         AS 状态码,
    SUM(pv)             AS 访问量
FROM log_analysis_db.nginx_log_result
GROUP BY status_code
ORDER BY 访问量 DESC;
