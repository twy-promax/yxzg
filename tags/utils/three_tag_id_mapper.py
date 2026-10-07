# coding:utf-8
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType


class ThreeTagIdMapper:
    """
    三级标签 id 映射器。

    匹配类标签的共同套路是：源表某个字段的值 → 三级标签的 rule → 三级标签的 id。
    本类按这个思路提供两个静态方法，直接用类名调用，不需要实例化：
        to_rule_id_dict：三级标签配置 DataFrame → {rule: id} 字典
        to_udf：生成"源字段值 → 三级标签 id"的 Spark UDF
    """

    @staticmethod
    def to_rule_id_dict(three_tag_df):
        """
        把三级标签配置转成 {rule字符串: 标签id} 字典。

        :param three_tag_df: 三级标签配置 DataFrame，须含 rule、id 两列
                            （由 TagBase.read_three_tag 提供）
        :return: 字典
        """
        # 单个二级标签下的三级标签只有个位数条，collect 到 Driver 端不会造成内存压力
        return {str(tag.rule): tag.id for tag in three_tag_df.collect()}

    @staticmethod
    def to_udf(three_tag_df, value_map=None, default_rule=None):
        """
        生成"源字段值 → 三级标签 id"的 Spark UDF。

        :param three_tag_df: 三级标签配置 DataFrame，须含 rule、id 两列
        :param value_map: 可选。源字段值到 rule 的映射字典。
                          用于源字段取值与 rule 语义对不上的标签，
                          例如政治面貌：源表 1=团员、3=群众，而三级标签 rule 只有 1=群众。
        :param default_rule: 可选。查不到时兜底使用的 rule，
                             同时承担字段空值（None）的归口，
                             例如职业字段 NULL 统一归到 rule='7'（其他）。
        :return: 可直接套用在列上的 UDF
        """
        rule_id_dict = ThreeTagIdMapper.to_rule_id_dict(three_tag_df)
        mapping = value_map or {}

        @F.udf(returnType=IntegerType())
        def _value_to_tag_id(value):
            # 1 - 统一转字符串：源字段可能是 int/bigint，而字典的键是字符串；
            #     空值会转成 'None'，自然落到后面的兜底分支
            key = str(value)

            # 2 - 若配置了值映射，先把源值翻译成三级标签的 rule
            if key in mapping:
                key = str(mapping[key])

            # 3 - 查表取标签 id
            tag_id = rule_id_dict.get(key)

            # 4 - 查不到（空值或脏值）时用兜底 rule 再查一次
            if tag_id is None and default_rule is not None:
                tag_id = rule_id_dict.get(str(default_rule))

            return tag_id

        return _value_to_tag_id
