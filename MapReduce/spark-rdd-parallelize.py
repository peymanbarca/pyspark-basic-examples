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

    tmp = ["Learn","Apache","Spark","with","Tutorial Kart","Diego MilitO"]
    items = sc.parallelize(tmp, numSlices=n_cores)  # Parallelize in RDD

    print(type(items))


    def func(item):
        print("\n* " + item)
        sleep(3)

    # in order to use threads, we must work as Spark RDD
    print(' ------------ Working In Spark RDD Space ---------------')
    items.foreach(lambda item : func(item))

    print(' ------------ Working After Collect RDD ---------------')

    # ---------- gather data (exit from RDD so spark parallelism not works!)
    gathered_data = items.collect()
    for item in gathered_data:
        func(item)