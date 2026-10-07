# coding:utf-8
import os
from pyspark.sql import Window
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from tags.base.tags_base import TagBase
from tags.utils.three_tag_id_joiner import ThreeTagIdJoiner

# 绑定Python解释器和Spark安装路径
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

class Payment(TagBase):
    # 参与"取最大渠道"比较的全部金额字段（对应源表的 10 个渠道金额列）
    AMOUNT_COLS = [
        "wechat_amount", "ali_pay_amount", "cash_amount", "balance_amount", "unionpay_amount",
        "point_amount", "member_card_amount", "gift_amount", "czapi_amount", "other_pay_amount",
    ]

    def compute(self, business_df:DataFrame, three_tag_df:DataFrame):
        # 1- 获取用户每笔订单的最常用支付方式：该笔订单中金额最大的那个渠道。
        #    全部用 DataFrame 原生函数实现
        #    先把金额字段的 null 统一按 0 处理，否则 greatest 与相等判断遇到 null 会跳过该渠道，导致结果偏移
        filled_df = business_df.fillna(0, subset=self.AMOUNT_COLS)

        #    后 5 个渠道按原逻辑合并成"其他"整体参与比较
        other_amount = (F.col("point_amount")+F.col("member_card_amount")+F.col("gift_amount")
                        +F.col("czapi_amount")+F.col("other_pay_amount"))

        #    该笔订单中的最大支付金额
        max_pay_amount = F.greatest(
            F.col("wechat_amount"), F.col("ali_pay_amount"), F.col("cash_amount"),
            F.col("balance_amount"), F.col("unionpay_amount"), other_amount
        )

        filled_df = filled_df.filter(max_pay_amount != 0)
        #    when 链按渠道的先后顺序依次判断，第一个金额等于最大值的渠道胜出。
        #    这与原实现用 ">" 严格比较、"平局保留先前渠道"的语义一致
        paytype_df = filled_df.select(
            F.col("zt_id"),
            F.when(F.col("wechat_amount") == max_pay_amount, "wechat")
             .when(F.col("ali_pay_amount") == max_pay_amount, "ali_pay")
             .when(F.col("cash_amount") == max_pay_amount, "cash")
             .when(F.col("balance_amount") == max_pay_amount, "balance")
             .when(F.col("unionpay_amount") == max_pay_amount, "unionpay")
             .otherwise("other")
             .alias("paytype")
        )

        # 2- 接着继续对数据进行处理，先统计每个用户每种支付方式使用的总次数分别是多少
        user_paytype_cnt_df = paytype_df.groupBy("zt_id","paytype").agg(
            F.count("paytype").alias("cnt")
        )

        # 3- 对每个用户的数据按照支付次数进行降序排序，取第一条数据
        new_business_df = user_paytype_cnt_df.select(
            "zt_id",
            "paytype",
            F.row_number().over(Window.partitionBy("zt_id").orderBy(F.desc("cnt"))).alias("rn")
        ).where("rn=1")

        # 4- 将处理后的业务数据与3级标签关联起来
        tag_new_df = ThreeTagIdJoiner.by_equal(
            new_business_df, three_tag_df, value_col="paytype"
        )

        return tag_new_df

if __name__ == '__main__':
    condition = "zt_id is not null and trade_date >= date_sub(current_date(), 90)"

    obj = Payment()
    obj.execute(two_tag_id=31, app_name="payment", where_condition=condition)
