#!/usr/bin/env python
# @desc :
__coding__ = "utf-8"
__author__ = "itcast team"

from log_generate.utils import ConfigLoader

from kafka import KafkaConsumer


def receiveData(topic_key, group_id):
    bsHost = ConfigLoader.getKafkaConfig("bootstrapServerHost")
    bsPort = ConfigLoader.getKafkaConfig("bootstrapServerPort")
    bootstrap_servers = [f'{bsHost}:{bsPort}']
    topic = ConfigLoader.getKafkaConfig(topic_key)
    # https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html
    consumer = KafkaConsumer(
        topic,
        group_id=group_id,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest'
    )

    for message in consumer:
        print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                             message.offset, message.key,
                                             message.value.decode('utf-8')), end='')
