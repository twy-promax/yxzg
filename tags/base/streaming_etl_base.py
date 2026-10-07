# coding:utf-8
from typing import Tuple
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.types import StringType
import pyspark.sql.functions as F
import os
import abc


# 绑定Python解释器和Spark安装路径
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

#实时数据ETL的基类
class StreamingETLBase(metaclass=abc.ABCMeta):
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

    #消费Kafka中的数据
    def read_kafka(self,spark,topic,starting_offsets="earliest"):
        # https://archive.apache.org/dist/spark/docs/3.3.2/structured-streaming-kafka-integration.html
        init_df = spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", "up01:9092") \
            .option("subscribe", topic) \
            .option("startingOffsets", starting_offsets) \
            .load()
        return init_df

    #对kafka中的value字段类型进行转换
    def convert_type(self,init_df):
        cast_type_df = init_df.select(init_df.value.cast(StringType()).alias("value"))
        return cast_type_df

    #ETL核心部分
    @abc.abstractmethod
    def etl(self,cast_type_df:DataFrame) -> Tuple[DataFrame,str]:
        # str为分区字段
        pass

    #输出数据到HDFS
    def write_hdfs(self,etl_df,hdfs_path,partition_field,appName):
        # checkpoint 用 writeStream 上的 .option 显式指定，路径才真正固定。
        # 若只在 SparkSession 里配 spark.sql.streaming.checkpointLocation，
        # Spark 会在该路径下再拼一个每次启动都不同的 UUID，等于每次重启都从头消费。
        # 本任务有两个 sink（HDFS + Kafka），它们必须各自独占一个 checkpoint 目录。
        etl_df.writeStream.format("orc").option("path", f"hdfs://up01:8020/xtzg/etl/{hdfs_path}") \
            .option("checkpointLocation", f"{self.CHK_ROOT}/{appName}_hdfs") \
            .partitionBy(partition_field) \
            .trigger(processingTime="5 seconds") \
            .start()

    #输出数据到Kafka，企业中一般会以JSON方式存放到Kafka中
    def write_kafka(self,etl_df,topic,appName):
        # 需要将DataFrame中的所有字段全部拼接为字符串
        fields = etl_df.columns
        kafka_df = etl_df.select(F.to_json(F.struct(*fields)).alias("value"))

        kafka_df.writeStream.format("kafka") \
            .option("checkpointLocation", f"{self.CHK_ROOT}/{appName}_kafka") \
            .option("kafka.bootstrap.servers", "up01:9092") \
            .option("topic", topic) \
            .start().awaitTermination()

    def execute(self,appName,read_topic,write_topic=None,hdfs_path=None):
        #创建SparkSession对象
        spark = self.create_SparkSession(appName)
        #消费kafka中的数据
        init_df = self.read_kafka(spark,topic=read_topic)
        #value字段的类型转换
        cast_type_df = self.convert_type(init_df)
        #etl核心
        etl_df,partition_field=self.etl(cast_type_df)
        #输出数据到HDFS
        if hdfs_path is not None:
            self.write_hdfs(etl_df,hdfs_path=hdfs_path,partition_field=partition_field,appName=appName)
        #输出数据到kafka
        if write_topic is not None:
            self.write_kafka(etl_df,topic=write_topic,appName=appName)


if __name__ == "__main__":
    pass
