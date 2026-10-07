-- 大屏 A 组件 A13 · 价格敏感度 PSM（横向条形图）｜pid=52｜字段 tags_id_times
-- 5 档：极度敏感 / 比较敏感 / 一般敏感 / 不太敏感 / 极度不敏感
-- ⚠️ PSM 有两条实现路线并存（统计类 psm_tag.py + 挖掘类 psm_ml.py），都写 53~57，最终值取决于哪条后跑

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
WHERE d.pid = 52 AND t.tag_id <> ''
GROUP BY TRIM(d.name), d.id
ORDER BY d.id;
