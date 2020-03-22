import sys
import os
import shutil

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3.6"

    # create Spark context with Spark configuration
    conf = SparkConf().setAppName("Word Count - Python").set("spark.hadoop.yarn.resourcemanager.address",
                                                             "0.0.0.0:8032").setMaster("local[2]").set("spark.executor.memory","2g");
    sc = SparkContext(conf=conf)

    # read in text file and split each document into words
    words = sc.textFile('/home/zevik/PycharmProjects/PySparkTutorial/SimpleMapReduce/input.txt').flatMap(lambda line: line.split(" "))

    # count the occurrence of each word
    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

    if os.path.isdir("/home/zevik/PycharmProjects/PySparkTutorial/SimpleMapReduce/output"):
        shutil.rmtree('/home/zevik/PycharmProjects/PySparkTutorial/SimpleMapReduce/output/')

    wordCounts.saveAsTextFile("/home/zevik/PycharmProjects/PySparkTutorial/SimpleMapReduce/output/")