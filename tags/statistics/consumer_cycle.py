# coding:utf-8
import os
from tags.base.tags_base import TagBase
from tags.utils.three_tag_id_joiner import ThreeTagIdJoiner
import pyspark.sql.functions as F

# 绑定Python解释器和Spark安装路径
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

class ConsumerCycle(TagBase):
    def compute(self,business_df, three_tag_df):
        # 得到用户上次的消费日期，并且与当前时间相减，最终得到消费周期
        last_business_df = business_df.groupby("zt_id").agg(
            F.max("trade_date").alias("last_trade_date")
        )
        day_diff_df = last_business_df.select(
            "zt_id",
            F.datediff(F.current_date(),"last_trade_date").alias("day_diff")
        )

        # 按消费周期天数区间关联 3 级标签，打上消费周期标签
        new_tag_df = ThreeTagIdJoiner.by_range(
            day_diff_df, three_tag_df, value_col="day_diff"
        )

        return new_tag_df

if __name__ == "__main__":
    condition = "zt_id is not null and trade_date >= date_sub(current_date(), 90)"
    obj = ConsumerCycle()
    obj.execute(two_tag_id=24,app_name="consumer_cycle",where_condition=condition,default_tag="30")
