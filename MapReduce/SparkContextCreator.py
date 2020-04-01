import sys
import os
from pyspark import SparkContext, SparkConf

def sparkContextCreator():
    os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3.6"

    # create Spark context with Spark configuration
    conf = SparkConf().setAppName("Read Text to RDD - Python").setMaster("local[8]").set("spark.executor.memory", "2g");
    sc = SparkContext(conf=conf)
    return sc