-- 大屏 A · KPI 卡 4 —— 标签类别数（字典里 level=2 的条数，当前 = 17）
-- 这 17 个类别就是大屏 A 上所有标签分布图的数量

SELECT COUNT(*) AS 标签类别数
FROM mysql_cat.tags_info.tbl_basic_tag
WHERE level = 2;
