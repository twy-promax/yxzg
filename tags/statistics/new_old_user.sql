with first_buy as (
    select
        zt_id,
        datediff(current_date,dt) as day_diff
    from dwm.dwm_mem_first_buy_i
    where dt <= date_sub(`current_date`(),1)
)
insert overwrite table ads.ads_mem_new_old_user_i partition(dt)
select
    distinct t2.zt_id,
    case
        when t1.day_diff <= 30 then 86
        when t1.day_diff > 30 then 87
        else 88
    end as tags_id,
    date_sub(current_date(),1) as dt
from dwm.dwm_sell_o2o_order_i t2
    left join first_buy t1 on t2.zt_id=t1.zt_id
where t2.zt_id is not null;

-- with first_buy as (
--     select
--         zt_id,
--         datediff(current_date,dt) as day_diff
--     from dwm.dwm_mem_first_buy_i
--     where dt <= date_sub(`current_date`(),1)
-- ),
-- t3 as (
--     select distinct
--     t2.zt_id,
--     case
--         when t1.day_diff <= 30 then 1
--         when t1.day_diff > 30 then 2
--         else 0
--     end as tags_id,
--     date_sub(current_date(),1) as dt
-- from dwm.dwm_sell_o2o_order_i t2
--     left join first_buy t1 on t2.zt_id=t1.zt_id
-- where t2.zt_id is not null
-- )
-- select
--     zt_id,
--     tags_id
-- from t3
-- where tags_id=0;