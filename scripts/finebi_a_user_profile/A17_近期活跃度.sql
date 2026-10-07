-- 大屏 A 组件 A17 · 近期活跃度（柱状图，实时）｜pid=127｜字段 tags_id_streaming（实时标签）
-- 3 档：高活跃度 / 中活跃度 / 低活跃度（按 12 小时内下单量分档）

SELECT
    TRIM(d.name)              AS 标签值,
    d.id                      AS 排序,
    COUNT(DISTINCT t.user_id) AS 用户数
FROM (
    SELECT user_id, tag_id
    FROM es_cat.default_db.user_profile_tags
    LATERAL VIEW explode_split(tags_id_streaming, ',') tmp AS tag_id
    WHERE tags_id_streaming IS NOT NULL AND tags_id_streaming <> ''
) t
JOIN mysql_cat.tags_info.tbl_basic_tag d
  -- 两边都按字符串比较：不对 ES 里的 token 做 CAST，空 token / 脏值只会关联不上，不会让整个查询报错
  ON CAST(d.id AS STRING) = TRIM(t.tag_id)
WHERE d.pid = 127 AND t.tag_id <> ''
GROUP BY TRIM(d.name), d.id
ORDER BY d.id;
