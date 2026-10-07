# coding:utf-8
import os

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from tags.base.tags_base import TagBase
from tags.utils.three_tag_id_joiner import ThreeTagIdJoiner

# 绑定Python解释器和Spark安装路径
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'


class ActivityTag(TagBase):
    """活跃度标签"""

    def compute(self, business_df: DataFrame, three_tag_df: DataFrame):
        """
        活跃度：按用户统计近 90 天的消费次数（订单数），再按次数区间打标签。

        源表 dwm_sell_o2o_order_i 一行就是一单（实测 order_no 唯一），
        所以 count(order_no) 就等于该用户的消费次数。

        :param business_df: 业务数据，含 zt_id、order_no
        :param three_tag_df: 活跃度下的三级标签配置（id、rule），rule 形如 "15-30"
        :return: 含 user_id、tags_id_times 两列的 DataFrame
        """
        # 1 - 按用户统计消费次数
        cnt_df = business_df.groupBy("zt_id").agg(
            F.count("order_no").alias("order_cnt")
        )

        # 2 - 按次数区间关联 3 级标签
        #     元数据的区间是 30-10000 / 15-30 / 5-15 / 0-5，相邻区间共享端点
        #     （30、15、5）。消费次数是整数，踩在端点上的用户比 PSM 那种浮点值多得多
        #     （实测 268 人），闭区间下会同时命中两条，所以打开 keep_leftmost
        return ThreeTagIdJoiner.by_range(
            cnt_df, three_tag_df, value_col="order_cnt", keep_leftmost=True
        )


if __name__ == "__main__":
    condition = "zt_id is not null and trade_date >= date_sub(current_date(), 90)"

    obj = ActivityTag()
    obj.execute(two_tag_id=47, app_name="activity_tag", where_condition=condition)
