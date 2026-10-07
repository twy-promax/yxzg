-- 大屏 B 组件 B11 · 关联规则 TOP10（表格）
-- 来源：recommend_db.fpgrowth_association_goods（同样只取最新一次计算）
-- antecedent / consequent 在 Doris 里是 ARRAY<STRING>，必须转成字符串才能上表格
-- 若 ARRAY_JOIN 报错（老版本 Doris），改用：CAST(antecedent AS STRING)

SELECT
    ARRAY_JOIN(antecedent, ',')  AS 前项商品,
    ARRAY_JOIN(consequent, ',')  AS 后项商品,
    ROUND(confidence, 3)         AS 置信度,
    ROUND(lift, 2)               AS 提升度,
    ROUND(support, 4)            AS 支持度
FROM recommend_db.fpgrowth_association_goods
WHERE calculate_date = (SELECT MAX(calculate_date) FROM recommend_db.fpgrowth_association_goods)
ORDER BY 提升度 DESC
LIMIT 10;
