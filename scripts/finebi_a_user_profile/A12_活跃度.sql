-- 大屏 A 组件 A12 · 活跃度（柱状图）｜pid=47｜字段 tags_id_times
-- 4 档：非常活跃 / 活跃 / 不活跃 / 非常不活跃
-- ⚠️ 实测约 94% 用户落在「非常不活跃」—— 文档的档位刻度是按 90 天活跃度设计的，样本撑不起来，属预期现象

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
WHERE d.pid = 47 AND t.tag_id <> ''
GROUP BY TRIM(d.name), d.id
ORDER BY d.id;
