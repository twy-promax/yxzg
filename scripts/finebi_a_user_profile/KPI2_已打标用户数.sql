-- 大屏 A · KPI 卡 2 —— 已打标用户数（三个标签字段任一非空即算已打标）

SELECT COUNT(DISTINCT user_id) AS 已打标用户数
FROM es_cat.default_db.user_profile_tags
WHERE COALESCE(tags_id_times, '') <> ''
   OR COALESCE(tags_id_once, '') <> ''
   OR COALESCE(tags_id_streaming, '') <> '';
