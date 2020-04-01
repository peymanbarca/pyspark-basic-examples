from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import os

def SparkContextCreator(NUM_CORE):
    os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3.6"

    conf = SparkConf().setAppName("Word Count - Python").set("spark.hadoop.yarn.resourcemanager.address",
                                                             "0.0.0.0:8032") \
        .setMaster("local[" + str(NUM_CORE) + "]").set("spark.executor.memory", "2g") \
        .set("spark.jars", "/home/zevik/PycharmProjects/pyspark-basic-examples/SparkSQL/postgresql-42.2.11.jar");

    sc = SparkContext(conf=conf)

    return sc,conf
