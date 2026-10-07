-- 大屏 A 组件 A14 · 新老会员（环形图）｜pid=85｜字段 tags_id_once（SQL 系标签）
-- 3 档：新会员 / 老会员 / 未消费会员
-- 注意 2 级标签在字典里的名称是「新老会员标签」，5 级才是新会员/老会员/未消费会员

SELECT
    TRIM(d.name)              AS 标签值,
    d.id                      AS 排序,
    COUNT(DISTINCT t.user_id) AS 用户数
FROM (
    SELECT user_id, tag_id
    FROM es_cat.default_db.user_profile_tags
    LATERAL VIEW explode_split(tags_id_once, ',') tmp AS tag_id
    WHERE tags_id_once IS NOT NULL AND tags_id_once <> ''
) t
JOIN mysql_cat.tags_info.tbl_basic_tag d
  -- 两边都按字符串比较：不对 ES 里的 token 做 CAST，空 token / 脏值只会关联不上，不会让整个查询报错
  ON CAST(d.id AS STRING) = TRIM(t.tag_id)
WHERE d.pid = 85 AND t.tag_id <> ''
GROUP BY TRIM(d.name), d.id
ORDER BY d.id;
