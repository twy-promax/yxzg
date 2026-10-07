#!/usr/bin/env python
# @desc :
__coding__ = "utf-8"
__author__ = "itcast team"

import sys
import os

from kafka import KafkaProducer
from kafka.errors import KafkaError

from log_generate.config import common
from log_generate.utils import FileUtils, ConfigLoader

producer_logger = common.get_logger('KafkaProducer')


def sendData(topic_key, path, prefix):

    bsHost = ConfigLoader.getKafkaConfig("bootstrapServerHost")
    bsPort = ConfigLoader.getKafkaConfig("bootstrapServerPort")
    # bsHosts = bsHost.split(",")
    # bootstrap_servers = [f'{bsHosts[0]}:{bsPort}', f'{bsHosts[1]}:{bsPort}', f'{bsHosts[2]}:{bsPort}']
    bootstrap_servers = [f'{bsHost}:{bsPort}']
    #event_topic=htv_insurance_user_event
    topic = ConfigLoader.getKafkaConfig(topic_key)
    """
    :param bootstrap_servers: kafka服务器地址：格式:[host:port, host2:port2, host3:port3]
    :param topic: 消息主题
    :param path:  待发送数据的所在路径
    :param prefix: 待发送数据文件的前缀
    :return: None
    """
    """
    常用其它参数：
    - client_id(str) 客户端字符串ID, Default: ‘kafka-python-producer-#’ (appended with a unique number per instance)
    - acks(0, 1, 'all')
    - compression_type(str) 压缩方式，可选：gzip, lz4, snappy, None 默认None
    - retries(int) 重试次数， 默认0
    - batch_size(数字) 批发送大小，默认：16384, 设置为0禁用批处理
    - value_serializer(函数) value的序列化逻辑
    注意：生产者为 异步发送
    """
    # 直接发送数据，无需转bytes，需要在构建producer的时候，设置value_serializer
    # 获取生产者对象，参数bootstrap_servers broker地址列表，传入list对象，内容是list['host:port', 'host:port', ......]
    producer = KafkaProducer(
        value_serializer=lambda m: bytes(m, encoding='utf-8'),
        bootstrap_servers=bootstrap_servers
    )

    # 发送数据
    # topic: 主题
    # value: 数据，注意要是字节数据，非中文数据可以 b'内容'直接转字节，中文需要： bytes('中文', encoding='utf-8')转换
    # key: key
    # headers: 数据headers
    # partition: 选择分区号
    # timestamp_ms: 时间戳（毫秒）
    # topic和value必选，其它可以不给
    # 返回值是future对象，注意是异步发送的
    try:
        datalist = FileUtils.readDataFromFile(path, prefix)

        for line in datalist[-5:]:
            producer.send(topic, line)
        # 阻塞当前代码执行，等待全部发送完成后再向后执行
        producer.flush()
        producer_logger.warning("向Kafka服务器:%s,发送最后5条数据,发送%s数据成功！！！", bootstrap_servers, prefix)
        producer.close()
    except KafkaError:
        print("error", file=sys.stderr)
        sys.exit(1)
