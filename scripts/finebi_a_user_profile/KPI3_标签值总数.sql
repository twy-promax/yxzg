-- 大屏 A · KPI 卡 3 —— 标签值总数（字典里 level=3 的条数，当前 = 82）
-- 来源：MySQL catalog 的标签字典表

SELECT COUNT(*) AS 标签值总数
FROM mysql_cat.tags_info.tbl_basic_tag
WHERE level = 3;
