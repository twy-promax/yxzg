-- 大屏 A 组件 A16 · 转化率（环形图，实时）｜pid=123｜字段 tags_id_streaming（实时标签）
-- 3 档：高转化率 / 中转化率 / 低转化率
-- ⚠️ 实时标签，覆盖的用户数远少于离线标签（只有被实时任务算到的用户才有值），
--    建议在大屏上标注「实时」并把这块的刷新频率与其它离线组件分开设置

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
WHERE d.pid = 123 AND t.tag_id <> ''
GROUP BY TRIM(d.name), d.id
ORDER BY d.id;
