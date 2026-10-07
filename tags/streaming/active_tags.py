# coding:utf-8
import os

from pyspark.sql import DataFrame

from tags.base.streaming_tag_base import StreamingTagBase
from tags.utils.three_tag_id_joiner import ThreeTagIdJoiner

# 绑定Python解释器和Spark安装路径
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'


class ActiveTag(StreamingTagBase):
    """近期活跃度标签（rule_id=127）"""

    def compute(self, business_df: DataFrame, three_tag_df: DataFrame, rule_obj):
        """
        近期活跃度：按 12 小时窗口内的下单量区间打标签。

        上游 topic `dws_shop_order_analysis` 的 order_num 已经是"该用户 12 小时内的下单量"
        （由 shop_order_analysis.py 的滑动窗口算出来），所以这里不用再做窗口聚合，
        直接拿 order_num 去匹配 3 级标签的区间即可。

        与转化率的差别：5 级区间 `4-1000` / `2-3` / `0-1` **互不共享端点**，
        同一个值不会命中两条标签，所以不需要传 keep_leftmost。

        :param business_df: 来自 Kafka 的业务数据，含 user_id、order_num
        :param three_tag_df: 近期活跃度下的三级标签配置（id、rule）
        :param rule_obj: 解析后的 2 级标签 rule（本方法用不到，保持基类签名一致）
        :return: 含 user_id、tags_id_streaming 两列的 DataFrame
        """
        result_df = ThreeTagIdJoiner.by_range(
            business_df, three_tag_df,
            value_col="order_num", user_col="user_id",
            tags_group="tags_id_streaming"
        )
        return result_df


if __name__ == "__main__":
    obj = ActiveTag()
    obj.execute("active_tag", 127)
