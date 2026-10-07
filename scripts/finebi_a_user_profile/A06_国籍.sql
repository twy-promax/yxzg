-- 大屏 A 组件 A06 · 国籍（横向条形图）｜pid=71｜字段 tags_id_times
-- 中国大陆 / 中国香港 / 中国澳门 / 中国台湾 / 其他（源字段是中文，代码里已转成 rule 编号）

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
WHERE d.pid = 71 AND t.tag_id <> ''
GROUP BY TRIM(d.name), d.id
ORDER BY d.id;
