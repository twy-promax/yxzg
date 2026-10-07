# coding:utf-8
import os
from tags.base.tags_base import TagBase
import pyspark.sql.functions as F
from pyspark.sql.types import *
from tags.utils.three_tag_id_mapper import ThreeTagIdMapper

# 绑定Python解释器和Spark安装路径
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

class GenderTag(TagBase):

    def compute(self,business_df, three_tag_df):
        # 6.1 - 生成打标签的 UDF：把 sex 字段值映射成三级标签 id
        #       sex 为3时按需求文档 5.4 的口径"空值填sex='0'"，
        #       归到 rule='0'，即元数据里的 id=7"未知"。
        get_sex_tagid=ThreeTagIdMapper.to_udf(three_tag_df, default_rule='0')

        # 6.2 - 调用
        new_tag_df = business_df.select(
            business_df["zt_id"].alias("user_id"),
            get_sex_tagid(business_df["sex"]).alias("tags_id_times")
        )
        return new_tag_df

if __name__ == "__main__":
    obj = GenderTag()
    obj.execute(two_tag_id=4,app_name="gender_tag")
