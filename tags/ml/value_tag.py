# coding:utf-8
import os

from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as F
from tags.base.tags_base import TagBase
from tags.utils.hdfs_util import HDFSUtil

# 绑定Python解释器和Spark安装路径
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'


class ValueTag(TagBase):
    """客户价值标签（挖掘类，K-Means 聚类）"""

    def compute(self, business_df: DataFrame, three_tag_df: DataFrame):
        """
        客户价值：先按规则给 R/F/M 打分，再用 K-Means 聚成 7 类，最后映射到 3 级标签。

        源表是订单表 dwm_sell_o2o_order_i
        （元数据 selectFields = zt_id,trade_date,order_no,real_paid_amount），
        所以三个指标的来源是：
            R = 最近一次下单距今的天数（对 recency_days 取 min）
            F = 下单次数（按 order_no 计数）
            M = 消费金额（real_paid_amount 求和）
        """

        # 1 - 计算R、F、M指标的值
        tmp_business_df = (business_df.withColumn(
            "recency_days",
            F.datediff(F.current_date(), F.to_date("trade_date"))
        )
        .groupBy("zt_id").agg(
            F.min("recency_days").alias("Recency"),
            F.count("order_no").alias("Frequency"),
            F.sum("real_paid_amount").alias("Monetary")
        ))

        # 2 - 根据RFM分别按照区间打分
        #     每段末档统一用 otherwise 收口，避免区间之间留下空档
        new_business_df = tmp_business_df.select(
            "zt_id",

            # R：<3→5分、3-6→4、6-10→3、10-15→2、否则1
            F.when(tmp_business_df["Recency"] < 3, 5)
            .when((tmp_business_df["Recency"] >= 3) & (tmp_business_df["Recency"] < 6), 4)
            .when((tmp_business_df["Recency"] >= 6) & (tmp_business_df["Recency"] < 10), 3)
            .when((tmp_business_df["Recency"] >= 10) & (tmp_business_df["Recency"] < 15), 2)
            .otherwise(1)
            .alias("Recency_score"),

            # F：≥32→5、24-32→4、16-24→3、8-16→2、否则1
            F.when(tmp_business_df["Frequency"] >= 32, 5)
            .when((tmp_business_df["Frequency"] >= 24) & (tmp_business_df["Frequency"] < 32), 4)
            .when((tmp_business_df["Frequency"] >= 16) & (tmp_business_df["Frequency"] < 24), 3)
            .when((tmp_business_df["Frequency"] >= 8) & (tmp_business_df["Frequency"] < 16), 2)
            .otherwise(1)
            .alias("Frequency_score"),

            # M：≥900→5、675-900→4、450-675→3、225-450→2、否则1
            F.when(tmp_business_df["Monetary"] >= 900, 5)
            .when((tmp_business_df["Monetary"] >= 675) & (tmp_business_df["Monetary"] < 900), 4)
            .when((tmp_business_df["Monetary"] >= 450) & (tmp_business_df["Monetary"] < 675), 3)
            .when((tmp_business_df["Monetary"] >= 225) & (tmp_business_df["Monetary"] < 450), 2)
            .otherwise(1)
            .alias("Monetary_score")
        )

        # 3 - 使用R、F、M的指标得分结合KMeans进行聚类
        # 3.1 - 对前面的业务数据进行特征处理
        assembler = VectorAssembler(
            inputCols=["Recency_score", "Frequency_score", "Monetary_score"],
            outputCol="features"
        )
        vector_df = assembler.transform(new_business_df)

        hdfs_path = "/spark_ml/kmeans"
        if HDFSUtil.isexists(hdfs_path):
            # 如果之前训练过，那么将历史数据重新加载进来
            kmeans_model = KMeansModel.load("hdfs://192.168.88.166:8020"+hdfs_path)
        else:
            # 3.2 - 创建KMeans算法
            kmeans = KMeans(featuresCol="features", predictionCol="prediction", k=7, seed=1)

            # 3.3 - 模型训练
            kmeans_model = kmeans.fit(vector_df)
            kmeans_model.save("hdfs://192.168.88.166:8020" + hdfs_path)
        kmeans_result = kmeans_model.transform(vector_df)

        # 4 - KMeans的结果和标签进行对应，给用户打上标签
        # 聚类编号（0~k-1）本身没有业务含义，不能直接当标签 id 用，要按"簇的价值高低"重新排：
        #   ① 聚类中心的 R/F/M 得分求和，和越大代表这簇用户价值越高
        centers_list_sum = [sum(c) for c in kmeans_model.clusterCenters()]
        # print('聚类中心点的和', centers_list_sum)
        #   ② 按和降序得到聚类编号的顺序 —— 这个顺序就是价值从高到低
        ordered_clusters = sorted(range(len(centers_list_sum)),
                                  key=lambda c: centers_list_sum[c], reverse=True)
        # print('聚类编号按价值降序', ordered_clusters)
        #   ③ 与 3 级标签 id 按升序配对（元数据里 id 越小价值越高：40 超高价值 → 46 超低价值）
        #      ⚠️ 必须显式 orderBy("id") —— collect() 不保证顺序
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
    # 元数据 id=39：inType=Hive##nodes=up01:9083##table=dwm.dwm_sell_o2o_order_i##selectFields=zt_id,trade_date,order_no,real_paid_amount##range=90
    condition = "zt_id is not null and trade_date >= date_sub(current_date(), 90)"
    obj = ValueTag()
    obj.execute(two_tag_id=39, app_name="value_tag", where_condition=condition, default_tag="46")
