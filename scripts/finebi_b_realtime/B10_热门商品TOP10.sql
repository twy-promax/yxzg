-- 大屏 B 组件 B10 · 热门商品 TOP10（表格 / 横向条形图）
-- 来源：recommend_db.popular_hot_goods（动态分区表，必须只取最新那一天，否则会把历史分区全加总）

SELECT
    goods_no            AS 商品编码,
    goods_name          AS 商品名称,
    third_category_name AS 三级品类,
    goods_num           AS 销量
FROM recommend_db.popular_hot_goods
WHERE recommend_date = (SELECT MAX(recommend_date) FROM recommend_db.popular_hot_goods)
ORDER BY 销量 DESC
LIMIT 10;
