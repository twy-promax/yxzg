# coding:utf-8
import os

from pyspark.ml.fpm import FPGrowth, FPGrowthModel
from pyspark.sql import SparkSession
import os
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, StructType, StructField, ArrayType

from tags.utils.hdfs_util import HDFSUtil

# 绑定Python解释器和Spark安装路径
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

# 基于FP-growth关联规则给用户推荐商品

def get_recommend_goods(current_goods_no, spark, fpg_model):
    """
        根据用户当前浏览的商品推荐关联商品
    :param current_goods_no: 用户当前浏览的商品ID
    :param spark: SparkSession对象
    :param fpg_model: FP-Growth算法模型
    :return: 关联商品
    """

    # 将普通的List变成DataFrame数据结构
    schema = StructType([
        StructField('items',ArrayType(StringType()),True)
    ])
    current_goods_df = spark.createDataFrame(data=[(current_goods_no,)],schema=schema)

    # 使用FP-growth模型进行关联商品推荐
    result_df = fpg_model.transform(current_goods_df)
    return result_df.collect()[0][1]


if __name__ == '__main__':
    # 1- 创建SparkSession对象
    spark = SparkSession.builder \
        .appName("fpgrowth_association_goods") \
        .master("local[*]") \
        .config("spark.sql.warehouse.dir", "hdfs://up01:8020/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://up01:9083") \
        .config("spark.sql.shuffle.partitions", 5) \
        .enableHiveSupport() \
        .getOrCreate()

    # 2- 数据输入：销售业务数据处理
    """
        目的：统计每笔订单中哪些商品是一起购买的
    """
    order_df = spark.sql("""
        select
            order_no,
            collect_set(goods_no) as items
        from dwm.dwm_sold_goods_sold_dtl_i
        where datediff(current_date(),to_date(trade_date))<=30 and goods_no is not null and order_no is not null
        group by order_no
    """)

    # 模型训练和保存
    hdfs_path = "/xtzg/recommend/fpg"
    if HDFSUtil.isexists(hdfs_path):
        # 如果之前训练过，那么将历史数据重新加载进来
        fpg_model = FPGrowthModel.load("hdfs://192.168.88.166:8020" + hdfs_path)
    else:
        # 3- 数据处理：创建FP-growth模型，对业务数据进行模型训练
        fpGrowth = FPGrowth(itemsCol="items", minSupport=0.0005, minConfidence=0.6)
        fpg_model = fpGrowth.fit(order_df)

        # 将训练结果存储到HDFS
        fpg_model.save("hdfs://192.168.88.166:8020"+hdfs_path)

    # 显示模型训练的结果：也就是看看哪些商品是有关联的
    rule_result = fpg_model.associationRules
    print(rule_result.count())
    rule_result.show()
    rule_result.printSchema()

    # 4- 数据输出：输出到Doris
    result_df = rule_result.withColumn(
        "calculate_date",
        F.current_timestamp()
    ).select(
        "calculate_date",
        rule_result["antecedent"].cast(StringType()).alias("antecedent"),
        rule_result["consequent"].cast(StringType()).alias("consequent"),
        "confidence","lift","support"
    )

    result_df.write.format("doris") \
        .option("doris.fenodes","up01:8130") \
        .option("doris.table.identifier", "recommend_db.fpgrowth_association_goods") \
        .option("user", "root") \
        .option("password", "123456") \
        .mode("append") \
        .save()

    # 5- 关联商品推荐
    current_goods_no = ["3215335"]
    recommod_result = get_recommend_goods(current_goods_no,spark,fpg_model)
    print(recommod_result)

    # 6- 释放资源
    spark.stop()