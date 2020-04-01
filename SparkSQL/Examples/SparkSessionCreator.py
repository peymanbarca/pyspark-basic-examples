from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import os

def SparkSessionCreator(NUM_CORE,conf):
    os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3.6"

    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.jars", "/home/zevik/PycharmProjects/pyspark-basic-examples/SparkSQL/postgresql-42.2.11.jar",
                conf=conf) \
        .getOrCreate()

    return spark