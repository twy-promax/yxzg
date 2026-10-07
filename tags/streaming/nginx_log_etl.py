# coding:utf-8
import os

import requests
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, MapType
import xml.etree.ElementTree as ET
from user_agents import parse
from tags.base.streaming_etl_base import StreamingETLBase

# 绑定Python解释器和Spark安装路径
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

class NginxLogETL(StreamingETLBase):
    def etl(self,cast_type_df):
        # 通过正则表达式转化字段
        pattern = ('(?<ip>\d+\.\d+\.\d+\.\d+) (- - \[)(?<datetime>[\s\S]+)(?<t1>\][\s"]+)(?<request>[A-Z]+) '
                   '(?<url>[\S]*) (?<protocol>[\S]+)["] (?<code>\d+) (?<sendbytes>\d+) ["](?<refferer>[\S]*) '
                   '["](?<useragent>[\S\s]+)["] ["](?<proxyaddr>[\S\s]+)["]')
        regexp_df = cast_type_df.select(
            F.regexp_extract("value", pattern, 1).alias("ip"),
            F.regexp_extract("value", pattern, 2).alias("cookie"),
            F.regexp_extract("value", pattern, 3).alias("datetime"),
            F.regexp_extract("value", pattern, 4).alias("t1"),
            F.regexp_extract("value", pattern, 5).alias("request"),
            F.regexp_extract("value", pattern, 6).alias("url"),
            F.regexp_extract("value", pattern, 7).alias("protocol"),
            F.regexp_extract("value", pattern, 8).alias("code"),
            F.regexp_extract("value", pattern, 9).alias("sendbytes"),
            F.regexp_extract("value", pattern, 10).alias("refferer"),
            F.regexp_extract("value", pattern, 11).alias("useragent"),
            F.regexp_extract("value", pattern, 12).alias("proxyaddr")
        )

        # 日期时间格式转换
        datetime_df = regexp_df.withColumn(
            "datetime",
            F.from_unixtime(F.unix_timestamp("datetime", "dd/MMM/yyyy:HH:mm:ss Z"), "yyyy-MM-dd HH:mm:ss")
        )

        # IP地址解析得到区域信息
        @F.udf(returnType=StringType())
        def parse_ip(ip_str):
            params = {
                "ip": ip_str,
                "output": "xml",
                "key": "dad23ee42f52b21f971515e931ae1148"
            }
            # 发送请求
            response = requests.get(url="https://restapi.amap.com/v3/ip", params=params, timeout=10)
            # 获取返回的XML文本
            xml_text = response.text
            # 解析XML
            tree = ET.fromstring(xml_text)
            status = tree.findtext("status")
            if status == "1":
                try:
                    province = tree.findtext("province")
                    city = tree.findtext("city")
                    if not province or not city:
                        return "内网或者国外"
                    return province + city
                except:
                    return "未知区域"
            else:
                return "未知区域"

        area_df = datetime_df.withColumn(
            "area",
            parse_ip("ip")
        )

        # UA解析
        """
            MapType就是字典类型，必须同时指定字典中key和value的值
        """

        @F.udf(returnType=MapType(keyType=StringType(), valueType=StringType()))
        def parse_ua(ua_str):
            result = parse(ua_str)

            os = result.os.family
            browser = result.browser.family
            device = result.device.family

            return {"os": os, "browser": browser, "device": device}

        tmp_etl_df = area_df.withColumn("os", parse_ua("useragent")["os"]) \
            .withColumn("browser", parse_ua("useragent")["browser"]) \
            .withColumn("device", parse_ua("useragent")["device"])


        etl_df = tmp_etl_df.withColumn("dt", F.substring("datetime", 1, 10))
        partition_field = "dt"
        return etl_df,partition_field

if __name__ == "__main__":
    obj = NginxLogETL()
    obj.execute(appName="nginx_log_etl",read_topic="xtzg_nginx_log",
                hdfs_path="dwd_nginx_etl_result",write_topic="dwd_nginx_etl_result")