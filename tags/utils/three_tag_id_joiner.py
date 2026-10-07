# coding:utf-8
import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.types import DoubleType


class ThreeTagIdJoiner:
    """
    三级标签 id 关联器。

    与 ThreeTagIdMapper 是一对，分别对应两种"业务数据 → 三级标签 id"的做法：
        ThreeTagIdMapper —— 用 UDF 把字段值映射成标签 id（适合枚举查表）
        ThreeTagIdJoiner —— 用 DataFrame join 把业务数据关联成标签 id（适合区间/等值匹配）

    两个静态方法产出的列名统一为 user_id、tags_id_times，与 TagBase 的约定一致。
    直接用类名调用，不需要实例化。
    """

    @staticmethod
    def by_range(business_df, three_tag_df, value_col, user_col="zt_id",
                 rule_sep="-", keep_leftmost=False,tags_group="tags_id_times"):
        """
        按数值区间关联。

        适用于 3 级标签 rule 形如 "start-end" 的标签，如年龄段、消费周期、PSM。

        :param tags_group:
        :param business_df: 处理后的业务数据
        :param three_tag_df: 三级标签配置（id、rule）
        :param value_col: 参与区间比较的字段名（如 birthday_date、day_diff、psm）
        :param user_col: 业务数据中的用户主键列名，默认 zt_id
        :param rule_sep: rule 中分隔 start/end 的符号，默认 "-"（年龄段、消费周期都是这个）；
                         PSM 价格敏感度的 rule 用的是 "~"，需要显式传入
        :param keep_leftmost: 同一用户命中多个区间时，是否只留"最左侧"（start 最小）的那条。
                              默认 False，保持原有行为；
                              PSM 的相邻区间共享端点（"0.6~1" 与 "1~3" 都含 1），
                              一个值会同时命中两条，需要传 True 消除重复打标
        :return: 含 user_id、tags_id_times 两列的 DataFrame
        """
        # 1 - 把 3 级标签的 rule 按分隔符切成 start、end 两列，并显式转成 double
        #     rule 取出来是字符串，不转类型就会退化成字符串比较（"10" < "9" 会判成真），
        #     数值比较才对得上
        new_three_tag_df = three_tag_df.select(
            "id",
            F.split("rule", rule_sep)[0].cast(DoubleType()).alias("start"),
            F.split("rule", rule_sep)[1].cast(DoubleType()).alias("end")
        )

        # 2 - 带区间条件关联
        joined_df = business_df.join(
            new_three_tag_df,
            on=(business_df[value_col] >= new_three_tag_df["start"])
               & (business_df[value_col] <= new_three_tag_df["end"])
        )

        # 3 - 命中多个区间时，只保留最左侧（start 最小）的那条
        if keep_leftmost:
            if business_df.isStreaming:
                # ⚠️ 流式 DataFrame 不支持 row_number() 这类"非时间窗口"，会直接报
                #    Non-time-based windows are not supported on streaming DataFrames。
                #    这里改用 groupBy + min(struct(start, id))：struct 按字段顺序比较，
                #    取到的就是 start 最小的那条，与最左侧区间的语义完全一致。
                #    注意流式聚合要求输出模式用 update（无 watermark 的聚合不能用 append）
                return joined_df.groupBy(
                    business_df[user_col].alias("user_id")
                ).agg(
                    F.min(F.struct(F.col("start"), F.col("id"))).alias("_leftmost")
                ).select(
                    "user_id",
                    F.col("_leftmost.id").alias(tags_group)
                )

            # 批式：用 row_number 窗口按 start 升序排名，只留最左侧那条
            joined_df = joined_df.withColumn(
                "rn",
                F.row_number().over(
                    Window.partitionBy(business_df[user_col])
                          .orderBy(new_three_tag_df["start"])
                )
            ).where("rn = 1")

        return joined_df.select(
            business_df[user_col].alias("user_id"),
            new_three_tag_df["id"].alias(tags_group)
        )

    @staticmethod
    def by_equal(business_df, three_tag_df, value_col, user_col="zt_id",tags_group="tags_id_times"):
        """
        按值相等关联。

        适用于 3 级标签 rule 是枚举值的标签，如支付方式、性别；
        若源字段取值与 rule 语义不一致（如政治面貌），应先用 ThreeTagIdMapper 转换后再传入。

        :param tags_group:
        :param business_df: 处理后的业务数据
        :param three_tag_df: 三级标签配置（id、rule）
        :param value_col: 参与等值比较的字段名（如 paytype）
        :param user_col: 业务数据中的用户主键列名，默认 zt_id
        :return: 含 user_id、tags_id_times 两列的 DataFrame
        """
        return business_df.join(
            three_tag_df,
            on=business_df[value_col] == three_tag_df["rule"]
        ).select(
            business_df[user_col].alias("user_id"),
            three_tag_df["id"].alias(tags_group)
        )
