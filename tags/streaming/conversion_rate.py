# coding:utf-8
import os
from tags.utils.three_tag_id_joiner import ThreeTagIdJoiner
from tags.base.streaming_tag_base import StreamingTagBase

# 绑定Python解释器和Spark安装路径
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

class ConversionRate(StreamingTagBase):
    def compute(self,business_df,three_tag_df,rule_obj):
        #处理业务数据，计算得到转化率
        new_business_df = business_df.select(
            "user_id",
            (business_df['buy_num'] / business_df['browse_num']).alias("conversion_rate")
        )
        #处理3级标签数据并打上标签
        result_df = ThreeTagIdJoiner.by_range(new_business_df, three_tag_df,
                                             value_col="conversion_rate", user_col="user_id",
                                             keep_leftmost=True, tags_group="tags_id_streaming")

        return result_df
if __name__ == "__main__":
    obj = ConversionRate()
    obj.execute("conversion_rate",123)


