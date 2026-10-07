# coding:utf-8
import os
from tags.base.tags_base import TagBase
from tags.utils.three_tag_id_mapper import ThreeTagIdMapper

# 绑定Python解释器和Spark安装路径
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'


class NationTag(TagBase):

    # 源表 address 字段存的是**中文**，而三级标签的 rule 是**数字**，类型对不上，必须转换。
    # 两者含义一一对应，转换表如下：
    #   中国大陆 → '1' → 标签"中国大陆"(id=72)
    #   中国香港 → '2' → 标签"中国香港"(id=73)
    #   中国澳门 → '3' → 标签"中国澳门"(id=74)
    #   中国台湾 → '4' → 标签"中国台湾"(id=75)
    #   其他     → '5' → 标签"其他"(id=76)
    ADDRESS_TO_RULE = {
        '中国大陆': '1',
        '中国香港': '2',
        '中国澳门': '3',
        '中国台湾': '4',
        '其他': '5',
    }

    def compute(self, business_df, three_tag_df):
        """
        国籍标签：把会员表的 address 字段（中文）转换后匹配成三级标签 id。

        :param business_df: 业务数据，含 zt_id、address 两列
        :param three_tag_df: 国籍下的三级标签配置（id、rule）
        :return: 含 user_id、tags_id_times 两列的 DataFrame
        """
        # 6.1 - 生成打标签的 UDF：把 address 字段值映射成三级标签 id
        #       传入 value_map 完成"中文 → rule 数字"的转换（见类属性 ADDRESS_TO_RULE）；
        #       实测 address 无空值、取值只有上述 5 种，
        #       default_rule 只作为将来出现新地址取值时的兜底，归到"其他"。
        get_nation_tagid = ThreeTagIdMapper.to_udf(
            three_tag_df,
            value_map=self.ADDRESS_TO_RULE,
            default_rule='5'
        )

        # 6.2 - 套用 UDF 打标签，并把主键列名对齐 TagBase 约定（zt_id → user_id）
        new_tag_df = business_df.select(
            business_df["zt_id"].alias("user_id"),
            get_nation_tagid(business_df["address"]).alias("tags_id_times")
        )

        return new_tag_df


if __name__ == "__main__":
    obj = NationTag()
    obj.execute(two_tag_id=71, app_name="nation_tag")
