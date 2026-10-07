# coding:utf-8
import os

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType
from tags.base.tags_base import TagBase
from tags.utils.three_tag_id_joiner import ThreeTagIdJoiner

# 绑定Python解释器和Spark安装路径
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'


class PSMTag(TagBase):
    """PSM 价格敏感度标签（统计口径）"""

    def compute(self, business_df: DataFrame, three_tag_df: DataFrame):
        """
        PSM 价格敏感度：三个比率相加得到一个敏感度分值，再按分值区间打标签。

        字段简写（对应源表的三个金额字段）：
            ra = order_total_amount  应收金额
            da = discount_amount     优惠金额
            pa = real_paid_amount    实付金额

        state 状态列：优惠金额大于 0（即这一单用了优惠）记 1，否则记 0。

        四个基础量（按 zt_id 聚合）：
            tdon = sum(state)    优惠订单数
            ton  = count(state)  总订单数
            tda  = sum(da)       优惠总金额
            tra  = sum(ra)       应收总金额

        三个指标：
            tdonr = tdon / ton                 优惠订单占比
            adar  = (tda / tdon) / (tra / ton) 平均优惠金额占比
            tdar  = tda / tra                  优惠总金额占比
            psm   = tdonr + adar + tdar


        :param business_df: 业务数据，含 zt_id、order_no、order_total_amount、
                            discount_amount、real_paid_amount
        :param three_tag_df: PSM 下的三级标签配置（id、rule），rule 形如 "0.6~1"
        :return: 含 user_id、tags_id_times 两列的 DataFrame
        """
        # 1 - 取出计算所需的列，主键保持 zt_id 原名不变
        #     下游的 ThreeTagIdJoiner.by_range 默认按 zt_id 找主键，这里改名会找不到
        temp_df = business_df.select(
            business_df["zt_id"],
            business_df["order_total_amount"].alias("ra"),
            business_df["discount_amount"].alias("da"),
            F.when(business_df["discount_amount"] > 0, 1).otherwise(0).alias("state")
        )

        # 2 - 按用户聚合出四个基础量
        total_df = temp_df.groupBy("zt_id").agg(
            F.sum("state").alias("tdon"),   # 优惠订单数
            F.count("state").alias("ton"),  # 总订单数
            F.sum("da").alias("tda"),       # 优惠总金额
            F.sum("ra").alias("tra")        # 应收总金额
        )

        # 3 - 按公式算出 psm
        rate_df = total_df.select(
            "zt_id",
            (
                (F.col("tdon") / F.col("ton"))
                + ((F.col("tda") / F.col("tdon")) / (F.col("tra") / F.col("ton")))
                + (F.col("tda") / F.col("tra"))
            ).alias("psm")
        )

        # 4 - 把 psm 钳制到 3 级标签的覆盖范围内，三种越界都要处理：
        #       - 分母为 0（用户一单优惠都没用过，tdon 就是 0）会算出 null → 按 0 处理
        #       - 优惠金额为负（退款冲抵）会把 psm 拉到 0 以下 → 归最低档"极度不敏感"
        #       - adar 在 tdon 远小于 ton 时会大于 1，psm 可能冲出上界 → 归最高档"极度敏感"
        max_end = three_tag_df.select(
            F.max(F.split("rule", "~")[1].cast(DoubleType())).alias("max_end")
        ).first()["max_end"]

        psm_df = rate_df.fillna(0, subset=["psm"]).withColumn(
            "psm",
            F.greatest(F.least(F.col("psm"), F.lit(max_end)), F.lit(0.0))
        )

        # 5 - 按 psm 区间关联 3 级标签
        #     - rule_sep：PSM 的 rule 用 "~" 分隔（如 "0.6~1"）而不是 "-"
        #     - keep_leftmost：相邻区间共享端点（"0.6~1" 和 "1~3" 都含 1），
        #       同一个 psm 值会同时命中两条，只保留最左侧那条
        return ThreeTagIdJoiner.by_range(
            psm_df, three_tag_df, value_col="psm", rule_sep="~", keep_leftmost=True
        )


if __name__ == "__main__":
    condition = "zt_id is not null and trade_date >= date_sub(current_date(), 90)"

    obj = PSMTag()
    obj.execute(two_tag_id=52, app_name="psm_tag", where_condition=condition)
