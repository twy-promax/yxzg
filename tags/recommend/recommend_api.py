# coding:utf-8
"""
云鲜智购 — 商品推荐接口服务（Flask）

对外提供三类接口：
    /recommend  按 FP-Growth 关联规则，根据当前浏览商品推荐关联商品；查不到规则时用热门商品兜底
    /hot        近期热门商品（读 Doris recommend_db.popular_hot_goods 的最新分区）
    /health     服务自检（模型是否加载、Spark 版本）

启动方式（⚠️ 不要用 spark-submit 起，否则 SparkSession 的 master 会和集群模式打架）：
    nohup python tags/recommend/recommend_api.py > recommend_api.log 2>&1 &

调用示例：
    http://192.168.88.166:5000/
    http://192.168.88.166:5000/recommend?goods_list=['3215330']
    http://192.168.88.166:5000/hot?limit=20
"""
import os
import ast

from flask import Flask, request, jsonify
from pyspark.ml.fpm import FPGrowthModel
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from tags.recommend.fpgrowth_association_goods import get_recommend_goods
from tags.utils.hdfs_util import HDFSUtil

# 绑定Python解释器和Spark安装路径
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

# FP-Growth 模型的落盘位置 —— 必须与 fpgrowth_association_goods.py 一致
MODEL_PATH = "/xtzg/recommend/fpg"
HDFS_PREFIX = "hdfs://up01:8020"

# Doris 连接：读数据走 FE 的 MySQL 协议端口 9030；
# 注意与写数据时的 format("doris") + doris.fenodes(up01:8130) 不是同一套接口
DORIS_URL = "jdbc:mysql://up01:9030/recommend_db?rewriteBatchedStatements=true&useSSL=false"
DORIS_PROPS = {
    "user": "root",
    "password": "123456",
    "driver": "com.mysql.jdbc.Driver",
}
HOT_TABLE = "popular_hot_goods"

# /hot 与兜底默认返回的商品数
DEFAULT_HOT_LIMIT = 20


def create_spark(app_name="recommend_api"):
    """
    创建 SparkSession。

    与其它推荐脚本写法保持一致（enableHiveSupport + local[*]），
    不走需求文档 6.7 提到的 AbstractTagsBase.get_spark —— 那个静态方法在 TagBase 里并不存在。
    """
    return SparkSession.builder \
        .config("spark.sql.warehouse.dir", "hdfs://up01:8020/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://up01:9083") \
        .config("spark.sql.shuffle.partitions", 5) \
        .appName(app_name) \
        .master("local[*]") \
        .enableHiveSupport() \
        .getOrCreate()


def load_fpg_model():
    """
    加载 FP-Growth 模型。

    先判存再加载：模型没训练过时给出可直接执行的提示，
    而不是抛一个难懂的 HDFS 异常（部署服务时这一步最省事）。
    """
    if not HDFSUtil.isexists(MODEL_PATH):
        raise SystemExit(
            "FP-Growth 模型不存在：%s\n"
            "请先在 Linux 侧跑一次训练脚本把模型落盘，再启动本服务：\n"
            "    spark-submit --master 'local[*]' --py-files tags.zip "
            "tags/recommend/fpgrowth_association_goods.py" % MODEL_PATH
        )
    return FPGrowthModel.load(HDFS_PREFIX + MODEL_PATH)


def parse_goods_list(raw):
    """
    解析 goods_list 参数，兼容三种写法：
        ['3215330']       需求文档里的写法（Python 列表字面量）
        3215330           单个商品
        3215330,3215331   逗号分隔

    ast.literal_eval 只在第一种写法下返回 list，
    传裸数字会得到 int、传逗号串会直接报错，所以这里统一兜住。

    :param raw: 原始查询串
    :return: 商品编码字符串列表
    :raise ValueError: 参数缺失或无法解析
    """
    if raw is None or raw.strip() == "":
        raise ValueError("缺少 goods_list 参数，示例：goods_list=['3215330']")

    text = raw.strip()
    if text.startswith("[") and text.endswith("]"):
        try:
            value = ast.literal_eval(text)
        except (ValueError, SyntaxError):
            raise ValueError("goods_list 不是合法的列表字面量：%s" % raw)
        if not isinstance(value, (list, tuple)):
            raise ValueError("goods_list 解析结果不是列表：%s" % raw)
        goods_list = [str(item).strip() for item in value]
    elif "," in text:
        goods_list = [item.strip() for item in text.split(",")]
    else:
        goods_list = [text]

    goods_list = [goods for goods in goods_list if goods != ""]
    if not goods_list:
        raise ValueError("goods_list 解析后为空：%s" % raw)
    return goods_list


def read_hot_goods(limit=DEFAULT_HOT_LIMIT):
    """
    读 Doris 里最近一次算出的近期热门商品，按销量倒序取前 limit 个。

    :param limit: 返回条数
    :return: [{'goods_no':.., 'goods_name':.., 'third_category_name':.., 'goods_num':..}, ...]
    """
    hot_df = spark.read.jdbc(url=DORIS_URL, table=HOT_TABLE, properties=DORIS_PROPS)

    # 表是动态分区，按日期分区存放，只取最新那天的结果
    latest_date = hot_df.agg(F.max("recommend_date")).first()[0]
    if latest_date is None:
        return []

    hot_rows = hot_df.where(F.col("recommend_date") == latest_date) \
        .orderBy(F.col("goods_num").desc()) \
        .limit(limit) \
        .collect()

    return [{"goods_no": row["goods_no"],
             "goods_name": row["goods_name"],
             "third_category_name": row["third_category_name"],
             "goods_num": row["goods_num"]} for row in hot_rows]


# ------------------------------ 服务初始化 ------------------------------
# ⚠️ 模块级只初始化一次 SparkSession 和模型。
#    Flask 的 debug 模式会起 reloader 把本模块再导入一遍，导致重复创建 SparkContext
#    （报 Only one SparkContext may be running），所以必须 debug=False、use_reloader=False。
spark = create_spark()
fpg_model = load_fpg_model()

app = Flask(__name__)

# 让 jsonify 直接输出中文而不是 \uXXXX（兼容 Flask 2.2 前后的两种写法）
try:
    app.json.ensure_ascii = False
except AttributeError:
    app.config["JSON_AS_ASCII"] = False


@app.route("/")
def index():
    return "欢迎来到云鲜智购商品推荐系统"


@app.route("/recommend")
def recommend():
    try:
        goods_list = parse_goods_list(request.args.get("goods_list"))
    except ValueError as error:
        return jsonify({"code": 400, "msg": str(error)}), 400

    try:
        recommend_goods = [str(goods_no) for goods_no in
                           get_recommend_goods(goods_list, spark, fpg_model)]

        # 关联规则是"买过 A 的人还买过 B"，新商品或订单量太少时查不到规则，
        # 这种情况用近期热门商品兜底，避免前端拿到空列表
        fallback = ""
        if not recommend_goods:
            try:
                recommend_goods = [str(item["goods_no"]) for item in read_hot_goods()]
                fallback = "hot_goods"
            except Exception as error:      # 兜底失败不影响主流程，但要在日志里看到原因
                print("读取热门商品兜底失败：%s" % error)
                fallback = "hot_goods_failed"

        return jsonify({"code": 0,
                        "goods_list": goods_list,
                        "recommend_goods": recommend_goods,
                        "count": len(recommend_goods),
                        "fallback": fallback})
    except Exception as error:
        return jsonify({"code": 500, "msg": "推荐失败：%s" % error}), 500


@app.route("/hot")
def hot():
    try:
        limit = int(request.args.get("limit", DEFAULT_HOT_LIMIT))
    except ValueError:
        return jsonify({"code": 400, "msg": "limit 必须是整数"}), 400

    try:
        return jsonify({"code": 0, "goods": read_hot_goods(limit)})
    except Exception as error:
        return jsonify({"code": 500, "msg": "读取热门商品失败：%s" % error}), 500


@app.route("/health")
def health():
    return jsonify({"code": 0,
                    "model_loaded": fpg_model is not None,
                    "model_path": MODEL_PATH,
                    "spark_version": spark.version})


if __name__ == "__main__":
    # threaded=False：请求串行处理，避免多线程共用同一个 JVM 模型对象
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=False)
