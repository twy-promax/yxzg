with t1 as (                                    -- 计算最近 30 天的去重用户总数
    select count(distinct zt_id) as user_count --总用户数
    from dwm.dwm_mem_member_behavior_day_i
    where datediff(current_date(),dt)<=30 and dt<=date_sub(current_date(),1)
),
t2 as (                                         -- 在最近 30 天的行为数据上，计算每个用户的原始 R、F、M 指标，以及全局汇总值
    select
        zt_id,
        datediff(current_date(), '2026-09-13')        as  r,     -- 距离当天的时间差
        sum(consume_times) over (partition by zt_id)  as   f,     -- 单个用户完单单量
        sum(consume_times) over ()                    as   sum_f, -- 所有用户完单单量
        sum(consume_amount) over (partition by zt_id) as   m,     -- 单个用户完单金额
        sum(consume_amount) over ()                   as   sum_m, -- 所有用户完单金额
        t1.user_count, -- 总用户数
        be.dt
    from dwm.dwm_mem_member_behavior_day_i be cross join t1
        where datediff(current_date(),be.dt)<=30 and be.dt<=date_sub(current_date(),1)
),
t3 as (                                         -- 将原始指标转换为 0/1 标志，并标记每个用户最新的一条记录
    select
         zt_id,
         if(min(r) over (partition by zt_id) < 7, 1, 0)           as  r,  -- 取最小的时间差作为r,如果小于7则为1
         if(f > sum_f * 1.00 / user_count, 1, 0)                  as  f,  -- 与平均值对比，如果大于平均值则为1
         if(m > sum_m * 1.00 / user_count, 1, 0)                  as  m,  -- 与平均值对比，如果大于平均值则为1
         row_number() over (partition by zt_id order by dt desc ) as  rn -- 按照日期进行逆序排列
    from t2
),
t4 as (                                         -- 根据 R、F、M 的组合映射到具体的 tags_id
    select
        zt_id,
        case when r = 1 and f = 1 and m = 1 then '114'
             when r = 1 and f = 0 and m = 1 then '115'
             when r = 0 and f = 1 and m = 1 then '116'
             when r = 0 and f = 0 and m = 1 then '117'
             when r = 1 and f = 1 and m = 0 then '118'
             when r = 1 and f = 0 and m = 0 then '119'
             when r = 0 and f = 1 and m = 0 then '120'
             when r = 0 and f = 0 and m = 0 then '121'
        end as tags_id
    from t3 where rn=1
)
insert overwrite table ads.ads_mem_user_rfm_i
select
    mem.zt_id,
    if(t4.zt_id is null, '121', t4.tags_id) as tags_id,
    date_sub(current_date(),1) as dt
from dwd.dwd_mem_member_union_i mem
left join t4 on mem.zt_id=t4.zt_id
where mem.start_date <= date_sub(current_date(),1) and mem.end_date >= date_format(date_sub(current_date(),1),'yyyy-MM-dd');
-- 因为end_date=9999-99-99不是合法日期，如果日期比较会被转换为null，所有要字符串比较