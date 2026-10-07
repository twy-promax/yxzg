# coding:utf-8
import os
from tags.base.tags_base import TagBase
from tags.utils.three_tag_id_mapper import ThreeTagIdMapper

# 绑定Python解释器和Spark安装路径
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'


class JobTag(TagBase):

    def compute(self, business_df, three_tag_df):
        """
        职业标签：把会员表的 job 字段匹配成三级标签 id。

        源字段与三级标签 rule 的对应关系（来自元数据，二者一致，无需额外映射）：
            1 学生(id=9)、2 公务员(10)、3 军人(11)、4 警察(12)、5 教师(13)、6 白领(14)

        :param business_df: 业务数据，含 zt_id、job 两列
        :param three_tag_df: 职业下的三级标签配置（id、rule）
        :return: 含 user_id、tags_id_times 两列的 DataFrame
        """
        # 6.1 - 生成打标签的 UDF：把 job 字段值映射成三级标签 id
        #       job 为空时归到 rule='7'，即元数据里的 id=84"其他"。
        #       实测 21208 条会员数据中有 8245 条 job 为 NULL（约 39%），
        #       若不兜底这部分用户会完全拿不到职业标签。
        get_job_tagid = ThreeTagIdMapper.to_udf(three_tag_df, default_rule='7')

        # 6.2 - 套用 UDF 打标签，并把主键列名对齐 TagBase 约定（zt_id → user_id）
        new_tag_df = business_df.select(
            business_df["zt_id"].alias("user_id"),
            get_job_tagid(business_df["job"]).alias("tags_id_times")
        )

        return new_tag_df


if __name__ == "__main__":
    obj = JobTag()
    obj.execute(two_tag_id=8, app_name="job_tag")
