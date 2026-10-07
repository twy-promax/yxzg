import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python'

if __name__ == '__main__':
    a = 5
    b = 3
    result = a / b
    print(round(result, 4))
