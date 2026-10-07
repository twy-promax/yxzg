-- 大屏 A 组件 A08 · RFM 用户分层（柱状图 / 热力）｜pid=113｜字段 tags_id_once（SQL 系标签，注意不是 tags_id_times）
-- 8 类：重要价值 / 重要发展 / 重要保持 / 重要挽留 / 一般价值 / 一般发展 / 一般保持 / 低价值
-- ⚠️ 字典里「重要挽留用户 」「一般发展用户 」带尾随空格，所以 TRIM 必须保留

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
WHERE d.pid = 113 AND t.tag_id <> ''
GROUP BY TRIM(d.name), d.id
ORDER BY d.id;
