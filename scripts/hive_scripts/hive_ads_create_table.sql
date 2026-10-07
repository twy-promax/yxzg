create database if not exists ads;
create table if not exists ads.ads_mem_new_old_user_i(
    user_id bigint comment "用户id",
    tags_id string comment "标签id"
)comment "新老用户标签表"
partitioned by (dt string comment "创建日期")
row format delimited fields terminated by ','
stored as orc
tblproperties ('orc.compress'='SNAPPY');

create table if not exists ads.ads_mem_user_rfm_i(
    user_id bigint comment "用户id",
    tags_id string comment "标签id"
)comment '用户rfm价值信息表'
partitioned by (dt string comment '创建日期')
row format delimited fields terminated by ','
stored as orc
tblproperties ('orc.compress'='SNAPPY');

create table if not exists ads.ads_mem_tags_i(
    user_id bigint comment "用户id",
    tags_id string comment "标签id"
)comment '会员标签表'
partitioned by (dt string comment '创建日期')
row format delimited fields terminated by ','
stored as orc
tblproperties ('orc.compress'='SNAPPY');