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


class UnitPriceTag(TagBase):
    """客单价标签"""

    def compute(self, business_df: DataFrame, three_tag_df: DataFrame):
        """
        客单价：先算每个用户的"平均每单实付金额"，再按金额区间打标签。

        源表 dwm_sell_o2o_order_i 一行就是一单（实测 16405 行对应 16405 个不同的 order_no），
        所以直接把 real_paid_amount 按用户求平均即可，不需要按 order_no 去重。

        :param business_df: 业务数据，含 zt_id、order_no、real_paid_amount
        :param three_tag_df: 客单价下的三级标签配置（id、rule），rule 形如 "15-30"
        :return: 含 user_id、tags_id_times 两列的 DataFrame
        """
        # 1 - 按用户算平均每单实付金额
        avg_df = business_df.groupBy("zt_id").agg(
            F.avg("real_paid_amount").alias("avg_price")
        )

        # 2 - 按金额区间关联 3 级标签
        #     元数据的区间是 0-5 / 5-15 / 15-30 / 30-60 / 60-100000，相邻区间共享端点
        #     （5、15、30、60），闭区间下一个均价会同时命中两条，所以打开 keep_leftmost
        return ThreeTagIdJoiner.by_range(
            avg_df, three_tag_df, value_col="avg_price", keep_leftmost=True
        )


if __name__ == "__main__":
    condition = "zt_id is not null and trade_date >= date_sub(current_date(), 90)"

    obj = UnitPriceTag()
    obj.execute(two_tag_id=78, app_name="unit_price_tag", where_condition=condition)
