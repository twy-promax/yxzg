# coding:utf-8
import os

import pyspark.sql.functions as F
from pyspark.sql.types import StringType, TimestampType

from tags.base.streaming_indicate_base import StreamingIndicateBase

# 绑定Python解释器和Spark安装路径
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'


class ShopOrderAnalysis(StreamingIndicateBase):

    def create_SparkSession(self, appName):
        """
        重写基类，关掉 whole-stage codegen。
        本任务的 json_parse 里有 8 个 select 表达式、每个都套着多个 when/otherwise，
        Spark 把整条查询编译成的那个 Java 方法会超过 JVM 单方法 64KB 的字节码上限，
        Janino 编译失败并抛 CodeGenerator: failed to compile
        （形如 expand_switchCaseCode_xxx is not declared in any enclosing class）。
        关掉后走解释执行路径，规避该问题（需求文档 5.7 也是这么建议的）。
        """
        spark = super().create_SparkSession(appName)
        spark.conf.set("spark.sql.codegen.wholeStage", "false")
        return spark

    def json_fields(self):
        """
        Debezium 报文是 before/after 两层嵌套，json_tuple 解析不了，
        所以这个字段列表用不上 —— 解析逻辑由下面重写的 json_parse 接管。
        """
        return ()

    def json_parse(self, type_df):
        """
        重写基类的 json_parse：Debezium 是嵌套 JSON，必须用 get_json_object 逐字段取
        （json_tuple 只支持一层，取不到 before.xxx / after.xxx）。
        """
        # 1- 解析JSON获取字段
        fields_df = type_df.select(
            F.get_json_object("value", "$.before.id").alias("before_id"),
            F.get_json_object("value", "$.before.buyer_id").alias("before_buyer_id"),
            F.get_json_object("value", "$.before.create_time").alias("before_create_time"),
            F.get_json_object("value", "$.before.parent_order_no").alias("before_parent_order_no"),
            F.get_json_object("value", "$.before.order_id").alias("before_order_id"),
            F.get_json_object("value", "$.before.order_total_amount").alias("before_order_total_amount"),
            F.get_json_object("value", "$.before.discount_amount").alias("before_discount_amount"),
            F.get_json_object("value", "$.before.real_paid_amount").alias("before_real_paid_amount"),

            F.get_json_object("value", "$.after.id").alias("after_id"),
            F.get_json_object("value", "$.after.buyer_id").alias("after_buyer_id"),
            F.get_json_object("value", "$.after.create_time").alias("after_create_time"),
            F.get_json_object("value", "$.after.parent_order_no").alias("after_parent_order_no"),
            F.get_json_object("value", "$.after.order_id").alias("after_order_id"),
            F.get_json_object("value", "$.after.order_total_amount").alias("after_order_total_amount"),
            F.get_json_object("value", "$.after.discount_amount").alias("after_discount_amount"),
            F.get_json_object("value", "$.after.real_paid_amount").alias("after_real_paid_amount"),

            F.get_json_object("value", "$.op").alias("op")
        )

        # 2- 按操作类型取值：删除（op='d'）取 before，其余取 after；
        #    三个金额字段一律取差值 after - before，null 当 0 处理
        json_df = fields_df.select(
            F.when(F.col('op') == 'd', F.col('before_id')).otherwise(F.col('after_id')).alias('id'),
            F.when(F.col('op') == 'd', F.col('before_parent_order_no')).otherwise(F.col('after_parent_order_no')).alias('parent_order_no'),
            F.when(F.col('op') == 'd', F.col('before_order_id')).otherwise(F.col('after_order_id')).alias('order_id'),
            F.when(F.col('op') == 'd', F.col('before_create_time')).otherwise(F.col('after_create_time')).cast(TimestampType()).alias('create_time'),
            (F.when(F.col('after_order_total_amount').isNull(), 0).otherwise(F.col('after_order_total_amount')) -
             F.when(F.col('before_order_total_amount').isNull(), 0).otherwise(F.col('before_order_total_amount'))).alias('order_total_amount'),
            (F.when(F.col('after_discount_amount').isNull(), 0).otherwise(F.col('after_discount_amount')) -
             F.when(F.col('before_discount_amount').isNull(), 0).otherwise(F.col('before_discount_amount'))).alias('discount_amount'),
            (F.when(F.col('after_real_paid_amount').isNull(), 0).otherwise(F.col('after_real_paid_amount')) -
             F.when(F.col('before_real_paid_amount').isNull(), 0).otherwise(F.col('before_real_paid_amount'))).alias('real_paid_amount'),
            F.when(F.col('op') == 'd', F.col('before_buyer_id')).otherwise(F.col('after_buyer_id')).alias('user_id')
        )

        # 3- 订单号：父单号为空时取子单号
        json_df = json_df.withColumn(
            'order_sn',
            F.when((F.col('parent_order_no').isNull()) | (F.col('parent_order_no') == ''),
                   F.col('order_id')).otherwise(F.col('parent_order_no'))
        )

        return json_df

    def indicate(self, parse_df):
        """
        指标计算：按用户做 12 小时滑动窗口的下单量等统计。
        定义窗口时要把 window 转成 String —— 它本身是 struct，不能直接作为分组列输出。
        """
        result_df = parse_df.withWatermark('create_time', '10 minutes') \
            .groupBy('user_id',
                     F.window('create_time', '12 hours', '1 minutes').cast(StringType()).alias('window_time')) \
            .agg(F.approx_count_distinct('order_sn').alias('order_num'),
                 F.sum('order_total_amount').alias('order_total_amount'),
                 F.sum('discount_amount').alias('discount_amount'),
                 F.sum('real_paid_amount').alias('real_paid_amount'))

        return result_df


if __name__ == '__main__':
    obj = ShopOrderAnalysis()
    obj.execute(appName="shop_order_analysis", read_topic="mysql_cdc.hive_data.shop_order",
                database_name="db_analysis_db", table_name="shop_order_analysis",
                write_topic="dws_shop_order_analysis")
