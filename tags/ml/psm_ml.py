# coding:utf-8
import os

from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.feature import VectorAssembler

from tags.base.tags_base import TagBase
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType
from tags.utils.hdfs_util import HDFSUtil

# 绑定Python解释器和Spark安装路径
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

class PSMMl(TagBase):
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
            (total_df["tdon"] / total_df["ton"]).alias("tdonr"),
            ((total_df["tda"] / total_df["tdon"]) / (total_df["tra"] / total_df["ton"])).alias("adar"),
            (total_df["tda"] / total_df["tra"]).alias("tdar")
        )

        psm_df = rate_df.fillna(0)

        # 4 - 使用K-Means进行聚类
        assembler = VectorAssembler(
            inputCols=["tdonr","adar","tdar"],
            outputCol="features"
        )
        vector_df=assembler.transform(psm_df)
        hdfs_path = "/spark_ml/psm"
        if HDFSUtil.isexists(hdfs_path):
            # 如果之前训练过，那么将历史数据重新加载进来
            kmeans_model = KMeansModel.load("hdfs://192.168.88.166:8020"+hdfs_path)
        else:
            # 4.2 - 创建KMeans算法
            kmeans = KMeans(featuresCol="features", predictionCol="prediction", k=5, seed=12)

            # 4.3 - 模型训练
            kmeans_model = kmeans.fit(vector_df)
            kmeans_model.save("hdfs://192.168.88.166:8020" + hdfs_path)
        kmeans_result = kmeans_model.transform(vector_df)

        # 5 - KMeans的结果和标签进行对应，给用户打上标签
        # 聚类编号（0~k-1）本身没有业务含义，不能直接当标签 id 用，要按"簇的敏感度高低"重新排：
        #   ① 三个特征（tdonr、adar、tdar）都是"越大越敏感"，所以把聚类中心的分量求和，
        #      和越大代表这簇用户越敏感
        centers_list_sum = [sum(c) for c in kmeans_model.clusterCenters()]
        # print('聚类中心点的和', centers_list_sum)
        #   ② 按和降序得到聚类编号的顺序 —— 这个顺序就是敏感度从高到低
        ordered_clusters = sorted(range(len(centers_list_sum)),
                                  key=lambda c: centers_list_sum[c], reverse=True)
        # print('聚类编号按敏感度降序', ordered_clusters)
        #   ③ 与 3 级标签 id 按升序配对（元数据里 id 越小越敏感：53 极度敏感 → 57 极度不敏感）
        #      ⚠️ 必须显式 orderBy("id") —— collect() 不保证顺序，标签 id 顺序一变就会静默错配
        #      ⚠️ 这个配对还依赖 k 与 3 级标签数量一致（都是 5），改 k 时要同步
        df_3_ids = [row.id for row in three_tag_df.orderBy("id").collect()]
        kmeans_and_tags_dict = dict(zip(ordered_clusters, df_3_ids))

        # print('聚类编号 → 标签id', kmeans_and_tags_dict)

        # 给用户贴上标签
        # 需要挨个到字典里查值，所以不能用 pandas_udf
        @F.udf(returnType=IntegerType())
        def cluster_index_2_tagsid(prediction):
            return kmeans_and_tags_dict[prediction]

        # 复用上面已经 transform 过的 kmeans_result
        result_df = kmeans_result.select(
            F.col("zt_id").alias("user_id"),
            cluster_index_2_tagsid(F.col("prediction")).alias("tags_id_times")
        )

        return result_df

if __name__ == "__main__":
    condition = "zt_id is not null and trade_date >= date_sub(current_date(), 90)"

    obj = PSMMl()
    obj.execute(two_tag_id=52, app_name="psm_ml", where_condition=condition)
