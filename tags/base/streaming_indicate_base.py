# coding:utf-8
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.types import StringType
import pyspark.sql.functions as F
import os
import abc


# 绑定Python解释器和Spark安装路径
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

#实时数据指标计算的基类
class StreamingIndicateBase(metaclass=abc.ABCMeta):
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
        # 注意：不显式指定 startingOffsets 时 Spark 默认是 latest，
        #       只消费"任务启动之后"到达的消息，Kafka 里已积压的历史数据会被全部跳过
        init_df = spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", "up01:9092") \
            .option("subscribe", topic) \
            .option("startingOffsets", starting_offsets) \
            .load()
        return init_df

    #对kafka中的value字段类型进行转换
    def convert_type(self,init_df):
        type_df = init_df.select(init_df.value.cast(StringType()).alias("value"))
        return type_df

    #子类实现：返回要从 Kafka value（JSON）中解析出的字段名列表
    @abc.abstractmethod
    def json_fields(self):
        pass
    
    #json结构的解析
    def json_parse(self,type_df):
        # 要解析的字段名必须由子类提供：type_df 此时只剩 value 一列，
        # 拿 type_df.columns 会得到 ["value"]，去 JSON 里找名为 value 的 key，什么都解析不出来
        fields = self.json_fields()
        parse_df = type_df.select(F.json_tuple("value",*fields).alias(*fields))
        return parse_df

    #指标计算核心部分
    @abc.abstractmethod
    def indicate(self,parse_df:DataFrame):
        pass

    #输出数据到Doris
    def write_2_doris(self,result_df,table_name,database_name,appName):
        def _write_2_doris_fn(batch_df:DataFrame,batch_id):
            batch_df.write.jdbc(
                url=f"jdbc:mysql://192.168.88.166:9030/{database_name}?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false",
                table=table_name,
                properties={'user': 'root', 'password': '123456'},
                mode="append"
            )
        # checkpoint 用 writeStream 上的 .option 显式指定路径才固定；
        # 只配 spark.sql.streaming.checkpointLocation 的话，Spark 会在其下再拼一个
        # 每次启动都不同的 UUID，等于每次重启都从头消费。
        # 本任务有两个 sink（Doris + Kafka），它们必须各自独占一个 checkpoint 目录。
        result_df.writeStream.foreachBatch(_write_2_doris_fn).outputMode("update") \
            .option("checkpointLocation", f"{self.CHK_ROOT}/{appName}_doris") \
            .start()


    #输出数据到Kafka，企业中一般会以JSON方式存放到Kafka中
    def write_kafka(self,result_df,topic,appName):
        # 需要将DataFrame中的所有字段全部拼接为字符串
        fields = result_df.columns
        kafka_df = result_df.select(F.to_json(F.struct(*fields)).alias("value"))

        kafka_df.writeStream.format("kafka") \
            .outputMode("update") \
            .option("checkpointLocation", f"{self.CHK_ROOT}/{appName}_kafka") \
            .option("kafka.bootstrap.servers", "up01:9092") \
            .option("topic", topic) \
            .start().awaitTermination()

    def execute(self,appName,read_topic,database_name="log_analysis_db",write_topic=None,table_name=None):
        #创建SparkSession对象
        spark = self.create_SparkSession(appName)
        #消费kafka中的数据
        init_df = self.read_kafka(spark,topic=read_topic)
        #value字段的类型转换
        cast_type_df = self.convert_type(init_df)
        #json结构的解析
        parse_df = self.json_parse(cast_type_df)
        #result核心
        result_df=self.indicate(parse_df)
        #输出数据到Doris
        if table_name is not None:
            # 按关键字传参：write_2_doris 的形参顺序是 (table_name, database_name, appName)，
            # 位置传参容易把 database_name 和 appName 传反
            self.write_2_doris(result_df,table_name,database_name=database_name,appName=appName)
        #输出数据到kafka
        if write_topic is not None:
            self.write_kafka(result_df,topic=write_topic,appName=appName)


if __name__ == "__main__":
    pass

