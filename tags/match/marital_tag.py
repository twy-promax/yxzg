# coding:utf-8
import os
from tags.base.tags_base import TagBase
from tags.utils.three_tag_id_mapper import ThreeTagIdMapper

# 绑定Python解释器和Spark安装路径
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'


class MaritalTag(TagBase):

    def compute(self, business_df, three_tag_df):
        """
        婚姻状况标签：把会员表的 marital_status 字段匹配成三级标签 id。

        源字段与三级标签 rule 的对应关系（来自元数据，二者一致，无需额外映射）：
            1 未婚(id=68)、2 已婚(69)、3 离异(70)

        :param business_df: 业务数据，含 zt_id、marital_status 两列
        :param three_tag_df: 婚姻状况下的三级标签配置（id、rule）
        :return: 含 user_id、tags_id_times 两列的 DataFrame
        """
        # 6.1 - 生成打标签的 UDF：把 marital_status 字段值映射成三级标签 id
        #       marital_status 为空时按需求文档 5.4 的口径"空值按未婚处理"，
        #       归到 rule='1'，即元数据里的 id=68"未婚"。
        get_marital_tagid = ThreeTagIdMapper.to_udf(three_tag_df, default_rule='1')

        # 6.2 - 套用 UDF 打标签，并把主键列名对齐 TagBase 约定（zt_id → user_id）
        new_tag_df = business_df.select(
            business_df["zt_id"].alias("user_id"),
            get_marital_tagid(business_df["marital_status"]).alias("tags_id_times")
        )

        return new_tag_df


if __name__ == "__main__":
    obj = MaritalTag()
    obj.execute(two_tag_id=67, app_name="marital_tag")
