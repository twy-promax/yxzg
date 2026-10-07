# coding:utf-8
import os

import pyspark.sql.functions as F
from tags.base.streaming_indicate_base import StreamingIndicateBase

# 绑定Python解释器和Spark安装路径
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'


class UserEventIndicate(StreamingIndicateBase):

    def json_fields(self):
        """告诉基类：要从 Kafka value 的 JSON 里解析出哪些字段"""
        return ("user_id", "user_name", "province", "is_browse", "is_order", "is_buy",
                "is_back_order", "is_cart", "browse_page", "browse_time", "to_time",
                "minimum_price", "page_keywords", "visit_time")

    def indicate(self, parse_df):
        """
        指标计算：13 个指标。
        流式 DataFrame 不支持 distinct 聚合，去重计数一律用 approxCountDistinct 代替。
        """
        result_df = parse_df.groupBy("user_id", "user_name").agg(
            F.approxCountDistinct("province").alias("area_num"),
            F.sum("is_browse").alias("browse_num"),
            F.sum("is_cart").alias("cart_num"),
            F.sum("is_order").alias("order_num"),
            F.sum("is_buy").alias("buy_num"),
            F.sum("is_back_order").alias("back_order_num"),
            F.approxCountDistinct("browse_page").alias("goods_num"),
            F.approxCountDistinct("browse_page").alias("browse_page_num"),
            # 停留时长（分钟）
            F.avg((F.unix_timestamp("to_time") - F.unix_timestamp("browse_time")) / 60).alias("stay_duration"),
            F.avg("minimum_price").alias("avg_price"),
            F.count("page_keywords").alias("keywords_num"),
            F.min("visit_time").alias("first_visit_time"),
            F.max("visit_time").alias("last_visit_time")
        )
        return result_df


if __name__ == "__main__":
    obj = UserEventIndicate()
    obj.execute(appName="user_event_indicate", read_topic="dwd_user_event_etl_result",
                table_name="user_event_result", write_topic="dws_user_event_analysis")
