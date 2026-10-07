-- 实现标签合并的操作
insert overwrite table ads.ads_mem_tags_i partition(dt)
select
    user_id,
    concat_ws(',',collect_list(tags_id)) as tags_id_once,
    date_sub(current_date(),1) as dt
from (select *
      from ads.ads_mem_new_old_user_i
      where dt = date_sub(current_date(), 1)
      union all
      select *
      from ads.ads_mem_user_rfm_i
      where dt = date_sub(current_date(), 1)
      ) tmp
group by user_id