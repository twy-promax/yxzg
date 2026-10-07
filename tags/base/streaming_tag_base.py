# coding:utf-8
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.types import StringType
import pyspark.sql.functions as F
import os
import abc
from tags.utils.rule_parse_util import RuleParse


# 绑定Python解释器和Spark安装路径
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

#实时数据ETL的基类
class StreamingTagBase(metaclass=abc.ABCMeta):
    # checkpoint 根目录：每个 sink 在其下占一个固定子目录，保证重启能接着上次的 offset 跑
    CHK_ROOT = "hdfs://up01:8020/streaming_chk"

    #创建SparkSession对象
    def create_SparkSession(self,appName):
        spark = SparkSession.builder \
            .appName(appName) \
            .master("local[*]") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()
        return spark

    #读取标签数据
    def get_mysql(self,spark):
        all_tag_df = spark.read.jdbc(
            url="jdbc:mysql://up01:3306/tags_info?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false",
            table="tbl_basic_tag",
            properties={
                "user": "root",
                "password": "123456",
            }
        )
        return all_tag_df

    #过滤出2级标签，并且解析rule规则
    def parse_tag_rule(self,all_tag_df,two_tag_id):
        rule_str = all_tag_df.where(f"id={two_tag_id}").first().rule
        rule_obj = RuleParse.parse(rule_str)
        
        return rule_obj

    #根据rule规则读取业务数据
    def read_kafka(self,spark,rule_obj):
        # https://archive.apache.org/dist/spark/docs/3.3.2/structured-streaming-kafka-integration.html
        # 元数据里 inType=Kafka 的标签，rule 的三个字段含义是：
        #   nodes → Kafka broker 地址、table → topic 名、range → startingOffsets
        # 注意是 rule_obj.nodes，不是 rule（RuleParse 没有 rule 属性）
        tmp_business_df = spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", rule_obj.nodes) \
            .option("subscribe", rule_obj.table) \
            .option("startingOffsets", rule_obj.range) \
            .load()
        return tmp_business_df

    #对kafka中的value字段类型进行转换
    def convert_type(self,tmp_business_df):
        type_df = tmp_business_df.select(tmp_business_df.value.cast(StringType()).alias("value"))
        return type_df

    #解析JSON，获取需要的字段
    def json_parse(self,type_df,rule_obj):
        # 要解析的字段名必须由子类提供：type_df 此时只剩 value 一列，
        # 拿 type_df.columns 会得到 ["value"]，去 JSON 里找名为 value 的 key，什么都解析不出来
        fields = tuple(rule_obj.selectFields.split(","))
        business_df = type_df.select(F.json_tuple("value",*fields).alias(*fields))
        return business_df

    #过滤并且读取对应的3级标签配置数据
    def read_three_tag(self, all_tag_df, two_tag_id):
        """
            过滤并且读取对应的3级标签配置数据
        """
        three_tag_df = all_tag_df.where(f"pid={two_tag_id}").select("id", "rule")
        return three_tag_df
    
    @abc.abstractmethod
    def compute(self, business_df:DataFrame, three_tag_df:DataFrame, rule_obj):
        """
            标签计算。将业务数据与3级标签进行关联，给用户打上3级标签
        :param business_df: 业务数据
        :param three_tag_df: 3级标签配置数据
        :return:
        """
        pass

    def add_update_time(self, result_df, appName):
        """
        追加标签更新时间列，列名约定为 {app_name}_time。
        实时场景取的是处理时间，用 F.current_timestamp()（流式里不能用 F.now()）。
        """
        return result_df.withColumn(
            f"{appName}_time", F.current_timestamp().cast(StringType())
        )

    def write_2_es(self, result_df, appName):
        """
            将标签计算结果数据存储到ES中
        """
        def write_2_es(batch_df: DataFrame, batch_id):
            batch_df.write.format("es").mode("append") \
                .option("es.nodes", "up01:9200") \
                .option("es.resource", "user_profile_tags") \
                .option("es.mapping.id", "user_id") \
                .option("es.write.operation", "upsert") \
                .save()

        # 输出模式用 update：实时标签的 compute 里通常带区间关联去重（groupBy 聚合），
        # append 模式不允许这种没有 watermark 的流式聚合。
        # checkpoint 必须用 writeStream 上的 .option 显式指定路径才固定；
        # 只配 spark.sql.streaming.checkpointLocation 的话，Spark 会在其下再拼一个
        # 每次启动都不同的 UUID，等于每次重启都从头消费。
        result_df.writeStream.foreachBatch(write_2_es).outputMode("update") \
            .option("checkpointLocation", f"{self.CHK_ROOT}/{appName}_es") \
            .start().awaitTermination()

    def execute(self,appName,two_tag_id):
        #创建SparkSession对象
        spark = self.create_SparkSession(appName)
        #读取标签数据
        all_tag_df = self.get_mysql(spark)
        #过滤出2级标签，并且解析rule规则
        rule_obj = self.parse_tag_rule(all_tag_df,two_tag_id)
        # 根据rule规则读取业务数据
        tmp_business_df = self.read_kafka(spark,rule_obj)
        #value字段的类型转换
        type_df = self.convert_type(tmp_business_df)
        #解析JSON，获取需要的字段
        business_df = self.json_parse(type_df,rule_obj)
        #过滤并且读取对应的3级标签配置数据
        three_tag_df = self.read_three_tag(all_tag_df,two_tag_id)
        #将业务数据与3级标签配置数据关联，给用户打上标签
        result_df = self.compute(business_df,three_tag_df,rule_obj)
        #追加标签更新时间列
        result_df = self.add_update_time(result_df,appName)
        #输出数据到ES
        self.write_2_es(result_df,appName)