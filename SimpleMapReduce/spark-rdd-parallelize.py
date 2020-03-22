import sys
import os

from pyspark import SparkContext, SparkConf
from time import sleep

if __name__ == "__main__":

    os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3.6"

    # create Spark context with Spark configuration
    n_cores = 4
    conf = SparkConf().setAppName("Read Text to RDD - Python").setMaster("local[" + str(n_cores) + "]").set("spark.executor.memory","2g");
    sc = SparkContext(conf=conf)

    tmp = ["Learn","Apache","Spark","with","Tutorial Kart"]
    items = sc.parallelize(tmp, numSlices=n_cores)

    print(type(items))


    def func(item):
        print("* " + item)
        sleep(10)

    # in order to use threads, we must work as Spark RDD
    items.foreach(lambda item : func(item))

    # ---------- gather data (exit from RDD so spark parallelism not works!)
    # gathered_data = items.collect()
    # for item in gathered_data:
    #     func(item)