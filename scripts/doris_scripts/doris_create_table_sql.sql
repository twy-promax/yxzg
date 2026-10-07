CREATE DATABASE IF NOT EXISTS log_analysis_db;
CREATE TABLE IF NOT EXISTS log_analysis_db.nginx_log_result
(
    ip varchar(15) comment 'ip地址',
    pv int comment 'pv数',
    uv int comment 'uv数',
    area varchar(50) comment '用户所在区域，根据ip解析',
    status_code varchar(10) comment '请求响应状态码',
    device_os varchar(50) comment '设备系统，从ua中提取手机或电脑使用的系统',
    device_brand varchar(50) comment '，从ua中提取手机或电脑的品牌',
    browser_name varchar(50) comment '电脑和手机，使用浏览器，记录浏览器简称',
    first_access_time datetime comment 'nginx日志记录首次访问时间',
    last_access_time datetime comment 'nginx日志记录末次访问时间'
)
UNIQUE KEY(ip)
DISTRIBUTED BY HASH(ip) BUCKETS 10
PROPERTIES("replication_num" = "1");

create table log_analysis_db.user_event_result(
    user_id bigint comment '用户id, 唯一标识',
    user_name varchar(25) comment '用户名',
    area_num bigint comment '区域（城市）数量',
    browse_num bigint comment '浏览行为数',
    cart_num bigint comment '加购行为数',
    order_num bigint comment '下单行为数',
    buy_num bigint comment '付款行为数',
    back_order_num bigint comment '退货行为数',
    goods_num bigint comment '浏览商品数',
    browse_page_num bigint comment '浏览页面数',
    stay_duration double comment '停留时间（分钟）',
    avg_price double comment '平均商品价格',
    keywords_num bigint comment '浏览的关键词数量',
    first_visit_time datetime comment '首次访问时间',
    last_visit_time datetime comment '末次访问时间'
)
UNIQUE KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 10
PROPERTIES("replication_num" = "1");

CREATE DATABASE IF NOT EXISTS db_analysis_db;
CREATE TABLE IF NOT EXISTS db_analysis_db.shop_order_analysis
(
    user_id bigint comment '用户id',
    window_time varchar(50) comment '窗口统计时间',
    order_num int comment '订单数',
    order_total_amount decimal(27, 2) comment '订单总金额',
    discount_amount decimal(27, 2) comment '优惠金额',
    real_paid_amount decimal(27, 2) comment '实付金额'
)
UNIQUE KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 10
PROPERTIES("replication_num" = "1");

create database if not exists recommend_db;
CREATE TABLE IF NOT EXISTS recommend_db.popular_hot_goods (
    recommend_date DATE comment '计算日期',
    goods_no bigint comment '商品编码',
    goods_name STRING comment '商品名称',
    third_category_no STRING comment '三级品类编码',
    third_category_name STRING comment '三级品类名称',
    goods_num INT comment '销售数量'
)
UNIQUE KEY(recommend_date, goods_no)
comment '热门商品推荐'
PARTITION BY RANGE(recommend_date) ()
DISTRIBUTED BY HASH(goods_no) BUCKETS 1
PROPERTIES (
    "dynamic_partition.create_history_partition" = "true",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-365",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "10",
    "replication_allocation" = "tag.location.default: 1"
);

select * from recommend_db.popular_hot_goods limit 100;

CREATE TABLE IF NOT EXISTS recommend_db.fpgrowth_association_goods
(   `calculate_date` DATETIME COMMENT "计算时间",
    `antecedent` ARRAY<STRING> COMMENT "购买的商品",
    `consequent` ARRAY<STRING> COMMENT "关联（推荐）商品",
    `confidence` DOUBLE COMMENT "置信度",
    `lift` DOUBLE COMMENT "提升度",
    `support` DOUBLE COMMENT "支持度"
)
DUPLICATE KEY(calculate_date)
comment '关联规则推荐'
PARTITION BY RANGE(calculate_date) ()
DISTRIBUTED BY HASH(calculate_date) BUCKETS 1
PROPERTIES (
    "dynamic_partition.create_history_partition" = "true",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-365",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "10",
    "replication_allocation" = "tag.location.default: 1"
);