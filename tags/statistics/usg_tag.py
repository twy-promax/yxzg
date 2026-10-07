# coding:utf-8
import os

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from tags.utils.three_tag_id_mapper import ThreeTagIdMapper
from tags.base.tags_base import TagBase

# 绑定Python解释器和Spark安装路径
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

class USGTag(TagBase):
    def compute(self,business_df:DataFrame, three_tag_df:DataFrame):
        # 1 - 确定每一条销售记录由男性购买的还是女性购买的
        tmp_business_df = business_df.select(
            "zt_id",
            F.when(F.expr("""
                third_category_no in ('20020105','20020101','20020303','20020304','20020201','20020801')
                    or
                goods_no in ('2904158','3224617','3229811','3231330','3222118')
            """),1)
            .when(F.expr("""
                third_category_no in ('20020102','20020402','10050507','10020403','30050301','30050401','30020901')
                    or
                goods_no in ('0301725','3221471','3224069','3225168','3232149','3216901')
            """),2)
            .otherwise(0)
            .alias("gender_type")
        )
        # 2- 统计每个用户下单的男性类的订单数、女性类的订单数、中性类的订单数和总订单数
        new_business_df = tmp_business_df.groupBy("zt_id").agg(
            F.count("zt_id").alias("total_cnt"),
            F.sum(F.when(F.col("gender_type") == 0,1).otherwise(0)).alias("middle_cnt"),
            F.sum(F.when(F.col("gender_type") == 1, 1).otherwise(0)).alias("male_cnt"),
            F.sum(F.when(F.col("gender_type") == 2, 1).otherwise(0)).alias("female_cnt")
        )
        # 3- 根据下单数占比判断购物性别
        usg_df = new_business_df.select(
            "zt_id",
            F.when((F.col('male_cnt') > F.col('female_cnt')) & (F.col('male_cnt') / F.col('total_cnt') >= 0.01),1)
            .when((F.col('female_cnt') > F.col('male_cnt')) & (F.col('female_cnt') / F.col('total_cnt') >= 0.01),2)
            .otherwise(0).alias("usg_type"))

        # 4 - 打上标签
        get_usg_tagid=ThreeTagIdMapper.to_udf(three_tag_df)

        new_tag_df = usg_df.select(
            usg_df["zt_id"].alias("user_id"),
            get_usg_tagid(usg_df["usg_type"]).alias("tags_id_times")
        )
        return new_tag_df

if __name__ == "__main__":
    condition = "zt_id != 0 and zt_id is not null and trade_date >= date_sub(current_date(), 90)"

    obj = USGTag()
    obj.execute(two_tag_id=58, app_name="usg_tag", where_condition=condition)
