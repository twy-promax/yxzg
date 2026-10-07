#!/usr/bin/env python
# @desc :
__coding__ = "utf-8"
__author__ = "itcast team"

from log_generate.utils import ProducerUtil

if __name__ == '__main__':
    # 每次发送最后5条数据到kafka
    # 相对路径
    ProducerUtil.sendData('nginx_topic', './source_data', 'access-nginx')
    # 绝对路径
    # ProducerUtil.sendData('nginx_topic', '/root/InsuranceUserProfile/source_data', 'access-nginx')
