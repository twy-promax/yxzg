# coding:utf-8
import os

import pyspark.sql.functions as F
from tags.base.streaming_indicate_base import StreamingIndicateBase

# 绑定Python解释器和Spark安装路径
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'


class NginxLogIndicate(StreamingIndicateBase):

    def json_fields(self):
        """告诉基类：要从 Kafka value 的 JSON 里解析出哪些字段"""
        return ("ip", "datetime", "request", "url", "protocol", "code", "sendbytes",
                "refferer", "useragent", "proxyaddr", "area", "os", "browser", "device")

    def indicate(self, parse_df):
        """
        指标计算：F.lit(1) 是在 DataFrame 末尾新增一列、值全为 1。
        这里已经按 ip 分过组，所以每组天然就是一个独立访客，uv 直接用 lit(1)。
        """
        result_df = parse_df.groupBy("ip").agg(
            F.count("ip").alias("pv"),
            F.lit(1).alias("uv"),
            F.first("area").alias("area"),
            F.first("code").alias("status_code"),
            F.first("os").alias("device_os"),
            F.first("device").alias("device_brand"),
            F.first("browser").alias("browser_name"),
            F.min("datetime").alias("first_access_time"),
            F.max("datetime").alias("last_access_time"),
        )
        return result_df


if __name__ == "__main__":
    obj = NginxLogIndicate()
    obj.execute(appName="nginx_log_indicate", read_topic="dwd_nginx_etl_result",
                table_name="nginx_log_result")
