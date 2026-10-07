# coding:utf-8
import os
from tags.base.tags_base import TagBase
from tags.utils.three_tag_id_joiner import ThreeTagIdJoiner
import pyspark.sql.functions as F

# 绑定Python解释器和Spark安装路径
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

class AgeTag(TagBase):

    def compute(self,business_df, three_tag_df):
        # 6.1 - 处理业务数据：去掉生日里的横杠，转成 yyyymmdd 便于与 3 级标签的区间比较
        new_business_df = business_df.withColumn("birthday_date", F.regexp_replace("birthday_date", "-", ""))

        # 6.2 - 按生日区间关联 3 级标签，打上年龄段标签
        new_tag_df = ThreeTagIdJoiner.by_range(
            new_business_df, three_tag_df, value_col="birthday_date"
        )

        return new_tag_df
if __name__ == "__main__":
    obj = AgeTag()
    obj.execute(two_tag_id=15,app_name="age_tag")