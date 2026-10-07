# coding:utf-8
import os

from pyspark.sql import SparkSession

# 绑定Python解释器和Spark安装路径
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == "__main__":
    spark = SparkSession.builder \
        .config("spark.sql.warehouse.dir", "hdfs://up01:8020/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://up01:9083") \
        .config("spark.sql.shuffle.partitions", 5) \
        .appName("popular_goods_hot") \
        .master("local[*]") \
        .enableHiveSupport() \
        .getOrCreate()

    # 数据获取
    recommend_df = spark.sql("""
        select
            current_date() as recommend_date,
            cast(goods_no as bigint) as goods_no,
            goods_name,
            third_category_name,
            third_category_no,
            cast(sum(sale_qty) as int) as goods_num
            from dwm.dwm_sold_goods_sold_dtl_i
        where datediff(current_date(),to_date(trade_date))<=30 and goods_no is not null
        group by third_category_name,third_category_no,goods_no,goods_name
        order by goods_num desc
    """)

    #数据输出
    recommend_df.write.format("doris") \
        .option("doris.fenodes","up01:8130") \
        .option("doris.table.identifier", "recommend_db.popular_hot_goods") \
        .option("user", "root") \
        .option("password", "123456") \
        .mode("append") \
        .save()

    # 释放资源
    spark.stop()

