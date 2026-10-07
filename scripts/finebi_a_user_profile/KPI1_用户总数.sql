-- 大屏 A · KPI 卡 1 —— 用户总数
-- 来源：ES catalog 的 user_profile_tags（一个 user_id 一行）

SELECT COUNT(DISTINCT user_id) AS 用户总数
FROM es_cat.default_db.user_profile_tags;
