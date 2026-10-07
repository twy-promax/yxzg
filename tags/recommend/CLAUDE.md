# tags/recommend — 推荐系统

> 包级说明。项目全局约定（运行环境、集群地址、通用陷阱）见项目根 `CLAUDE.md`；
> 基类见 `tags/base/CLAUDE.md`。

## 职责

离线推荐计算 + 推荐接口服务。推荐结果写入 **Doris `recommend_db`**。

## 现状：部分完成 —— 2 条算法线 + Flask 接口

| 文件 | 角色 | 输出 | 状态 |
|---|---|---|---|
| `popular_goods_hot.py` | 近期热门商品（基于流行度） | Doris `recommend_db.popular_hot_goods` | ✅ 已完成 |
| `fpgrowth_association_goods.py` | FP-Growth 关联规则（训练 + 落盘模型 + 写规则表 + `get_recommend_goods` 封装） | Doris `recommend_db.fpgrowth_association_goods`；模型 HDFS `/xtzg/recommend/fpg` | ✅ 已完成 |
| `recommend_api.py` | **Flask 接口服务**（`/`、`/recommend`、`/hot`、`/health`） | — | ✅ 已完成 |

> **本项目只做这两条线**。个人热门、ALS、ItemCF、UserCF **已决定不做**（详见下节）。

> ⚠️ **命名与需求文档全面不一致**（以代码为准）：
>
> | 文档 6.x 规划 | 实际 |
> |---|---|
> | `popular_hot_goods_recommend.py` | `popular_goods_hot.py` |
> | `fp_growth_recommend.py` | `fpgrowth_association_goods.py` |
> | `recommend_server.py` | `recommend_api.py` |
> | `popular_person_hot_goods_recommend.py`、`als_recommend.py`、`user_cf_recommend.py` | 不做 |

## 已放弃的算法线（文档 6.3(2) / 6.4 / 6.5）

| 算法 | 文档输出表 | 决定 |
|---|---|---|
| 个人热门商品 | `popular_person_hot_goods` | ❌ 不做 |
| ALS 隐语义协同过滤 | `als_goods_for_user` | ❌ 不做 |
| ItemCF 物品协同过滤 | `als_sim_goods_list` | ❌ 不做 |
| UserCF 用户协同过滤 | `user_cf_goods_for_user` | ❌ 不做 |

**影响**：`recommend_db` 实际只用到 **2 张表**（不是文档 6.8 说的 6 张），
建表脚本见 `scripts/doris_scripts/doris_create_table_sql.sql:54-91`（只建了这两张）。
需求文档 6.3–6.5 的原始设计仍保留在那里，仅作参考。

## 前置依赖：需求文档 6.2 与实际代码的差异（重要）

| 文档 6.2 要求 | 实际状况 |
|---|---|
| `AbstractTagsBase.get_spark('<appName>')`（静态） | **不存在**。`TagBase` 只有实例方法 `init_spark`，且硬编码 `master("local[*]")`。本包三个脚本都是**自建** `SparkSession`（`enableHiveSupport` + `master("local[*]")`） |
| `AbstractTagsBase.read_es(spark, fields=...)`（静态） | **不存在**（本包也没用到 —— 用它的是已放弃的 UserCF） |
| `write_to_doris(df, dbtable, ...)` 统一写出方法 | **从未实现**。实际是各脚本直接用 `write.format("doris")` 写，参数见下 |
| `tags/utils/hdfs_util.py` 的 `HdfsUtil().exists()` | ✅ 已实现，但类名/方法名是 **`HDFSUtil.isexists()`**（调用时直接用类名，不要实例化） |

## 公共约定（实际代码）

**写 Doris**（`popular_goods_hot.py`、`fpgrowth_association_goods.py` 都用这套）：

```python
df.write.format("doris") \
  .option("doris.fenodes", "up01:8130") \
  .option("doris.table.identifier", "recommend_db.<表名>") \
  .option("user", "root").option("password", "123456") \
  .mode("append").save()
```

**读 Doris**（只有 `recommend_api.py` 的 `/hot` 用）：JDBC 走 FE 的 MySQL 协议端口 **9030**，
`jdbc:mysql://up01:9030/recommend_db`，driver `com.mysql.jdbc.Driver`。

> ⚠️ **8130（写）与 9030（读）是两套不同接口，别混**：前者是 Spark-Doris 连接器的 FE 接入点，后者是 MySQL 协议端口。

**Doris 表公共属性**：动态分区 `dynamic_partition.enable=true`、`time_unit=DAY`、`start=-365`、`end=3`、`prefix=p`、`replication_allocation=tag.location.default:1`。

## 近期热门商品（`popular_goods_hot.py`）

与文档 6.3 的差异（**以代码为准**，且代码与建表 DDL 是对得上的）：

| 项 | 代码实际 | 文档 6.3 |
|---|---|---|
| 文件名 | `popular_goods_hot.py` | `popular_hot_goods_recommend.py` |
| 热度的度量 | **`sum(sale_qty) as goods_num`**（销售数量） | `count(order_no) as order_num`（订单数） |
| 日期列 | `recommend_date` | `calculate_date` |
| 商品标识 | 直接用 `cast(goods_no as bigint)` | 虚拟 id `cast(concat('1', goods_no) as int)` |
| 时间窗 | `datediff(current_date(), to_date(trade_date)) <= 30` | `dt >= date_sub(current_date,30) and dt <= date_sub(current_date,1)` |
| 条数限制 | 无 `limit` | `limit 1000` |
| 用户过滤 | 无 | 排除 `zt_id != 0 / is null` |

> 表结构以 `scripts/doris_scripts/doris_create_table_sql.sql:55-76` 为准：
> 列名就是 `recommend_date / goods_no / goods_name / third_category_no / third_category_name / goods_num`，
> 主键 `UNIQUE(recommend_date, goods_no)` —— 所以是**代码与 DDL 一致、与文档 6.3 不一致**。

## FP-Growth 关联规则（`fpgrowth_association_goods.py`）

- 输入：`select order_no, collect_set(goods_no) as items from dwm.dwm_sold_goods_sold_dtl_i
  where datediff(current_date(), to_date(trade_date)) <= 30 ... group by order_no`
  —— 时间窗是 **30 天**，文档 6.6 写的 90 天**以代码为准**。
- 参数：`FPGrowth(itemsCol="items", minSupport=0.0005, minConfidence=0.6)` ✔ 与文档 6.6 一致。
- **模型落盘路径 `/xtzg/recommend/fpg`**（判存用 `HDFSUtil.isexists`）——
  文档 6.6/6.8 写的是 `/model/fpGrowthModel`，**以代码为准**；`recommend_api.py` 必须按这个路径加载。
  > ⚠️ 模型一旦落盘就**永久复用**（`load` 分支优先），要重训必须先删掉该目录。
  > 另外脚本里 `save/load` 用的是 `hdfs://192.168.88.166:8020`，接口服务用 `hdfs://up01:8020` —— 同一个 NameNode，等价。
- 封装了 **`get_recommend_goods(goods_list, spark, fpg_model)`**，接口服务直接 import 复用；
  该函数把商品列表包成单行 DataFrame 交给 `fpg_model.transform()`，取 `collect()[0][1]`（即 `prediction` 列）。
- 规则表列 `calculate_date / antecedent / consequent / confidence / lift / support` ✔ 与 DDL 一致。

## 接口服务（`recommend_api.py`）

| 路由 | 说明 | 返回示例 |
|---|---|---|
| `/` | 欢迎页 | `欢迎来到云鲜智购商品推荐系统` |
| `/recommend?goods_list=['3215330']` | 按关联规则推荐；**查不到规则时用热门商品兜底** | `{"code":0,"goods_list":["3215330"],"recommend_goods":[...],"count":2,"fallback":""}` |
| `/hot?limit=20` | 读 Doris 最新分区的热门商品，按销量倒序 | `{"code":0,"goods":[{"goods_no":..,"goods_name":..,"goods_num":..}]}` |
| `/health` | 服务自检 | `{"code":0,"model_loaded":true,"model_path":"/xtzg/recommend/fpg","spark_version":"3.3.2"}` |

- `goods_list` 参数做了**容错解析**：`['3215330']`、`3215330`、`3215330,3215331` 三种写法都接受；
  非法或缺失返回 `400`（文档 6.7 只写了 `ast.literal_eval`，那种写法只支持第一种）。
- 兜底发生时 `fallback` 为 `hot_goods`；兜底本身失败则为 `hot_goods_failed`（原因打印在服务日志里）。

**部署步骤**（Linux 侧）：

```bash
# 0. ⚠️ 所有命令都必须在【项目根目录】下执行 —— 这是最容易踩的坑，见下面第 1 条
cd /export/data/workspace/user_profile

# 1. 前置：先跑一次训练脚本把模型落盘，否则服务启动时直接退出并给出提示
spark-submit --master 'local[*]' --py-files tags.zip tags/recommend/fpgrowth_association_goods.py

# 2. 依赖
pip install Flask

# 3. 启动（⚠️ 用 -m 跑，不要用 spark-submit；日志写绝对路径，免得在别处找不到）
nohup python -m tags.recommend.recommend_api > /export/data/workspace/user_profile/recommend_api.log 2>&1 &

# 4. 验证（先确认进程与端口，再判断接口 —— Spark 起会话要几秒到十几秒）
ps -ef | grep recommend_api
curl "http://192.168.88.166:5000/"
curl "http://192.168.88.166:5000/health"
curl "http://192.168.88.166:5000/recommend?goods_list=['3215330']"
curl "http://192.168.88.166:5000/hot?limit=5"
```

**四个必踩的坑**：

1. ⚠️ **必须在项目根目录用 `python -m` 启动**：`recommend_api.py` 里有 `from tags.recommend... import ...`，
   而 `python <绝对路径>/recommend_api.py` 时 Python 只把**脚本所在目录**加进 `sys.path`、**不含当前目录** ——
   从 `/root` 之类的目录启动会立刻 `ModuleNotFoundError: No module named 'tags'`（进程 Exit 1、端口起不来）。
   正确写法见上面第 3 步；调试时建议先前台跑 `python -m tags.recommend.recommend_api`，报错能直接看到。
2. ⚠️ **必须 `debug=False`**：Flask 的 debug 模式会启动 reloader 把模块**再导入一次**，
   于是创建第二个 `SparkContext`，直接报 `Only one SparkContext may be running`。本文件固定了 `debug=False`。
3. ⚠️ **不要用 `spark-submit` 起这个服务**：脚本里硬编码了 `master("local[*]")`，用 `spark-submit` 会和集群模式打架。
4. ⚠️ **`/hot` 与兜底需要 MySQL JDBC 驱动**：`recommend_db` 走 9030 的 JDBC 读，
   驱动 jar 在 `jar包/` 下（5.1.49 / 8.0.28 两个版本并存，注意驱动类名要与所放 jar 匹配），
   必须放进 Spark 的 `jars/` 目录才生效。

## 模型持久化策略汇总

**只有 FP-Growth 持久化模型，路径 `/xtzg/recommend/fpg`**（不是文档写的 `/model/fpGrowthModel`）；
其余算法线已放弃，不涉及模型。
