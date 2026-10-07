# coding:utf-8
import os

import pyspark.sql.functions as F
from tags.base.streaming_etl_base import StreamingETLBase

# 绑定Python解释器和Spark安装路径
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'


class UserEventETL(StreamingETLBase):
    def etl(self, cast_type_df):
        """
        解析用户行为日志的 JSON，抽成独立的列。
        json_tuple 与 get_json_object 的区别：
            1- json_tuple
                优点：一次能够同时解析多个字段
                缺点：针对嵌套的 JSON，需要一层一层的解析
            2- get_json_object
                优点：针对嵌套的 JSON，不管嵌套多少层都能解析
                缺点：一次只能解析一个字段
        本日志有嵌套结构（如 area.province、user_behavior.is_browse），所以用 get_json_object。
        """
        parse_df = cast_type_df.select(
            F.get_json_object("value", "$.user_id").alias("user_id"),
            F.get_json_object("value", "$.user_name").alias("user_name"),
            F.get_json_object("value", "$.area.province").alias("province"),
            F.get_json_object("value", "$.user_behavior.is_browse").alias("is_browse"),
            F.get_json_object("value", "$.user_behavior.is_order").alias("is_order"),
            F.get_json_object("value", "$.user_behavior.is_buy").alias("is_buy"),
            F.get_json_object("value", "$.user_behavior.is_back_order").alias("is_back_order"),
            F.get_json_object("value", "$.user_behavior.is_cart").alias("is_cart"),
            F.get_json_object("value", "$.goods_detail.browse_page").alias("browse_page"),
            F.get_json_object("value", "$.goods_detail.browse_time").alias("browse_time"),
            F.get_json_object("value", "$.goods_detail.to_time").alias("to_time"),
            F.get_json_object("value", "$.minimum_price").alias("minimum_price"),
            F.get_json_object("value", "$.goods_detail.page_keywords").alias("page_keywords"),
            F.get_json_object("value", "$.visit_time").alias("visit_time"),
        )

        # 增加分区字段
        etl_df = parse_df.withColumn("dt", F.substring("visit_time", 1, 10))
        partition_field = "dt"
        return etl_df, partition_field


if __name__ == "__main__":
    obj = UserEventETL()
    obj.execute(appName="user_event_etl", read_topic="xtzg_user_event",
                hdfs_path="dwd_user_event_etl_result", write_topic="dwd_user_event_etl_result")
