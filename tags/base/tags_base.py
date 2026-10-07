# coding:utf-8
from pyspark.sql import SparkSession, DataFrame
import pandas as pd
import pyspark.sql.functions as F
import os

from pyspark.sql.types import StringType

from tags.utils.rule_parse_util import RuleParse
import abc

# 绑定Python解释器和Spark安装路径
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

class TagBase(metaclass=abc.ABCMeta):

    def init_spark(self,app_name):
        """
            创建SparkSession对象
        """
        spark = SparkSession.builder \
            .config("spark.sql.warehouse.dir", "hdfs://up01:8020/user/hive/warehouse") \
            .config("hive.metastore.uris", "thrift://up01:9083") \
            .config("spark.sql.shuffle.partitions", 5) \
            .appName(app_name) \
            .enableHiveSupport() \
            .getOrCreate()

        return spark

    def get_mysql(self,spark):
        """
            读取所有的标签配置数据
        """
        all_tag_df = spark.read.jdbc(
            url="jdbc:mysql://up01:3306/tags_info?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false",
            table="tbl_basic_tag",
            properties={
                "user": "root",
                "password": "123456",
            }
        )
        return all_tag_df

    def parse_tag_rule(self,all_tag_df, two_tag_id):
        """
            读取并且解析2级标签的rule规则
        """
        rule_str = all_tag_df.where(f"id={two_tag_id}").first()["rule"]
        result_obj = RuleParse.parse(rule_str)

        return result_obj

    def read_hive(self,spark, result_obj,where_condition):
        """
            读取hive中的业务数据
        :param where_condition: 对业务数据的过滤条件
        :return:
        """
        hive_query_sql = "select " + result_obj.selectFields + " from " + result_obj.table + " where 1=1"
        if where_condition is not None:
            hive_query_sql = hive_query_sql + " and " + where_condition
        business_df = spark.sql(hive_query_sql)

        return business_df

    def read_three_tag(self,all_tag_df, two_tag_id):
        """
            过滤并且读取对应的3级标签配置数据
        """
        three_tag_df = all_tag_df.where(f"pid={two_tag_id}").select("id", "rule")
        return three_tag_df

    @abc.abstractmethod
    def compute(self,business_df, three_tag_df):
        """
            标签计算：将业务数据与3级标签进行关联，给用户打上3级标签
        :param business_df: 业务数据
        :param three_tag_df: 3级标签配置数据
        :return:
        """
        pass

    def read_es(self,spark):
        """
            读取ES中旧的标签数据
        """
        old_tag_df = spark.read.format("es") \
            .option("es.nodes", "up01:9200") \
            .option("es.resource", "user_profile_tags") \
            .option("es.read.field.include", "user_id,tags_id_times") \
            .load()
        return old_tag_df

    @staticmethod
    @F.pandas_udf(StringType())
    def merge_new_and_old_tagid(new_tagid:pd.Series,old_tagid:pd.Series,all_three_tagid:pd.Series,default_tagid:pd.Series) -> pd.Series:
        def _merge_one(new_tagid,old_tagid,all_three_tagid,default_tagid):
            # 0 - 空值判断（含"全量用户"语义的默认值兜底）
            #     本轮没算到（new 为空）的标签用默认值兜底；
            #     default_tagid 为空串 = 该标签不要求全量语义，行为与改造前完全一致
            if pd.isna(new_tagid) and default_tagid:
                new_tagid = default_tagid
            if pd.isna(new_tagid):
                return old_tagid
            elif pd.isna(old_tagid):
                return new_tagid
            # 1 - 将新旧标签ID数据类型全部转字符串
            new_tagid_str = str(new_tagid)
            old_tagid_str = str(old_tagid)
            all_three_tagid_str = str(all_three_tagid)

            # 2 - 将新旧ID标签数据按逗号切分为List列表
            new_tagid_list = new_tagid_str.split(",")
            old_tagid_list = old_tagid_str.split(",")
            all_three_tagid_list = all_three_tagid_str.split(",")

            # 先从旧标签结果数据中将当前2级标签下所有属于3级标签的全部删除
            old_tagid_list = [oldid for oldid in old_tagid_list if oldid not in all_three_tagid_list]

            # 3 - 合并：两个List列表相加即可得到新List
            all_tagid_list = new_tagid_list + old_tagid_list

            # 4 - 去重并排序
            result_list = sorted({int(float(x)) for x in all_tagid_list})

            # 5 - 按逗号拼接成字符串
            return ",".join(str(x) for x in result_list)

        return pd.Series(
            [_merge_one(new_tagid, old_tagid,all_three_tagid,default_tagid) for new_tagid,old_tagid,all_three_tagid,default_tagid in zip(new_tagid, old_tagid,all_three_tagid,default_tagid)],
            index = new_tagid.index
        )

    def merge_tag(self,new_tag_df:DataFrame,old_tag_df:DataFrame,three_tag_df:DataFrame,default_tag=None):
        """
        实现新旧标签结果数据的合并操作

        :param default_tag: 未覆盖用户补的默认 3 级标签 id（字符串）。
                            只给需要"全量用户"语义的标签传（消费周期 '30'、客户价值 '46'）；
                            不传或传空 = 保持原有行为（本轮没算到的用户原样保留旧标签）
        """

        # 得到当前2级标签下的所有3级标签，并且按照逗号拼接成字符串
        # all_three_tagid_list=three_tag_df.rdd.map(lambda row:str(row.id)).collect()
        all_three_tagid_list = [str(row.id) for row in three_tag_df.collect()]
        all_three_tagid = ",".join(all_three_tagid_list)
        # 新旧数据进行full join
        # join 用的是列名字符串（USING 语义）：结果里 user_id 只有一列，
        # full join 下取值来自非空的那一侧，所以不需要再 F.coalesce 一次
        new_df = new_tag_df.alias("n")
        old_df = old_tag_df.alias("o")
        result_df = new_df.join(
            old_df,on="user_id",how="full"
        ).select(
            F.col("user_id"),
            TagBase.merge_new_and_old_tagid(F.col("n.tags_id_times"),F.col("o.tags_id_times"),F.lit(all_three_tagid),F.lit(default_tag or "")).alias("tags_id_times"),
        )
        return result_df

    def write_2_df(self,result_df):
        """
            将标签结果数据存储到ES中
        """
        result_df.write.format("es").mode("append") \
            .option("es.nodes", "up01:9200") \
            .option("es.resource", "user_profile_tags") \
            .option("es.mapping.id", "user_id") \
            .option("es.write.operation", "upsert") \
            .save()

    def execute(self,two_tag_id,app_name,where_condition=None,default_tag=None):
        # 1 - 创建SparkSession对象
        spark = self.init_spark(app_name)
        # 2 - 读取标签配置的所有数据
        all_tag_df = self.get_mysql(spark)
        # 3 - 从所有标签配置中过滤出年龄段的2级标签,解析rule规则
        result_obj = self.parse_tag_rule(all_tag_df, two_tag_id)
        # 4 - 根据解析后的rule规则读取业务数据
        business_df = self.read_hive(spark, result_obj,where_condition)
        # 5 - 根据二级标签过滤3级标签，只需保留id,rule两个字段
        three_tag_df = self.read_three_tag(all_tag_df, two_tag_id)
        # 6 - 将业务数据和3级标签配置数据关联，给用户打上标签
        new_tag_df = self.compute(business_df, three_tag_df)
        # 7 - 读取ES旧的标签数据
        old_tag_df = self.read_es(spark)
        # 8 - 将新旧标签合并
        result_df = self.merge_tag(new_tag_df, old_tag_df,three_tag_df,default_tag)
        # 9 - 将标签数结果数据输出到ES中
        self.write_2_df(result_df)
        # 10 - 释放资源
        spark.stop()


if __name__ == "__main__":
    pass
