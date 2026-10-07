-- 大屏 A 组件 A15 · 购物性别 USG（环形图，可选）｜pid=58｜字段 tags_id_times
-- ⚠️ 强烈建议不上屏：实测约 99% 用户是「中性」
--    原因是 USG 依赖的 10 个特征编码（8 个品类 + 2 个商品）在现有数据源里完全不存在，属数据源问题，不是代码问题。
--    如果一定要放，建议只做一角小环或直接换成文字说明。

SELECT
    TRIM(d.name)              AS 标签值,
    d.id                      AS 排序,
    COUNT(DISTINCT t.user_id) AS 用户数
FROM (
    SELECT user_id, tag_id
    FROM es_cat.default_db.user_profile_tags
    LATERAL VIEW explode_split(tags_id_times, ',') tmp AS tag_id
    WHERE tags_id_times IS NOT NULL AND tags_id_times <> ''
) t
JOIN mysql_cat.tags_info.tbl_basic_tag d
  -- 两边都按字符串比较：不对 ES 里的 token 做 CAST，空 token / 脏值只会关联不上，不会让整个查询报错
  ON CAST(d.id AS STRING) = TRIM(t.tag_id)
WHERE d.pid = 58 AND t.tag_id <> ''
GROUP BY TRIM(d.name), d.id
ORDER BY d.id;
