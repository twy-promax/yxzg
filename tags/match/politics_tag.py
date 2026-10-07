# coding:utf-8
import os
from tags.base.tags_base import TagBase
from tags.utils.three_tag_id_mapper import ThreeTagIdMapper

# 绑定Python解释器和Spark安装路径
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'


class PoliticsTag(TagBase):

    # 源表 politics 字段的取值含义与三级标签 rule 的编号含义**不一致**，必须重映射。
    #   源表 job 注释：1团员、2党员、3群众、4其他党派
    #   三级标签 rule：1群众、2党员、3其他党派
    # 注意"3"的含义在两处正好互换，直接拿字段值当 rule 用会把"群众"打成"其他党派"。
    #
    # 映射依据（需求文档 5.4：团员归属"群众"）：
    #   源值 1(团员) ┐
    #   源值 3(群众) ┴→ rule '1' → 标签"群众"(id=64)
    #   源值 2(党员)  → rule '2' → 标签"党员"(id=65)
    #   源值 4(其他党派) → rule '3' → 标签"其他党派"(id=66)
    POLITICS_TO_RULE = {
        '1': '1',
        '3': '1',
        '2': '2',
        '4': '3',
    }

    def compute(self, business_df, three_tag_df):
        """
        政治面貌标签：把会员表的 politics 字段映射后匹配成三级标签 id。

        :param business_df: 业务数据，含 zt_id、politics 两列
        :param three_tag_df: 政治面貌下的三级标签配置（id、rule）
        :return: 含 user_id、tags_id_times 两列的 DataFrame
        """
        # 6.1 - 生成打标签的 UDF：把 politics 字段值映射成三级标签 id
        #       传入 value_map 完成源值到 rule 的翻译（见类属性 POLITICS_TO_RULE 的说明）；
        #       实测 politics 无空值，default_rule 只作为脏数据的兜底，归到"群众"。
        get_politics_tagid = ThreeTagIdMapper.to_udf(
            three_tag_df,
            value_map=self.POLITICS_TO_RULE,
            default_rule='1'
        )

        # 6.2 - 套用 UDF 打标签，并把主键列名对齐 TagBase 约定（zt_id → user_id）
        new_tag_df = business_df.select(
            business_df["zt_id"].alias("user_id"),
            get_politics_tagid(business_df["politics"]).alias("tags_id_times")
        )

        return new_tag_df


if __name__ == "__main__":
    obj = PoliticsTag()
    obj.execute(two_tag_id=63, app_name="politics_tag")
