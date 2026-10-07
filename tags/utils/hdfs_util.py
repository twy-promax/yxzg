# coding:utf-8
import os
import pyhdfs

# 绑定Python解释器和Spark安装路径
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

class HDFSUtil(object):
    @staticmethod
    def isexists(input_path):
        """
            判断指定HDFS的路径是否存在数据
        """
        return pyhdfs.HdfsClient(hosts="up01:9870",user_name="root").exists(input_path)

if __name__ == "__main__":
    pass
