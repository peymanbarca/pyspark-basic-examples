from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import os
import json
import time

if __name__ == "__main__":
    os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3.6"

    n_cores = 3

    conf = SparkConf().setAppName("Word Count - Python").set("spark.hadoop.yarn.resourcemanager.address","0.0.0.0:8032")\
        .setMaster("local[" + str(n_cores) + "]").set("spark.executor.memory", "2g")\
        .set("spark.jars", "/home/zevik/PycharmProjects/PySparkTutorial/SimpleSparkSQL/postgresql-42.2.11.jar");

    sc = SparkContext(conf=conf)

    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.jars", "/home/zevik/PycharmProjects/PySparkTutorial/SimpleSparkSQL/postgresql-42.2.11.jar",conf=conf) \
        .getOrCreate()


    t1=time.time()

    query = 'select organization_id,click_ts,impression_ts  from organization_listing_report_ts limit 10'

    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://xxx:5432/kilid") \
        .option("query", query) \
        .option("user", "xxx") \
        .option("password", "xxx") \
        .option("driver",'org.postgresql.Driver')\
        .option("numPartitions",1)\
        .load()

    df.printSchema()
    print(type(df))
    #df.show()

    df_p = spark.createDataFrame(sc.parallelize(df.collect(),numSlices=n_cores))
    print('number of partiotions is : ',df_p.rdd.getNumPartitions())


    # ----------- working with DataFrame (High Level RDD API from Spark) --------
    def processRowOfDF(row):
        organization_id = row['organization_id']
        click_ts = row['click_ts']
        click_ts = json.loads(click_ts)
        impression_ts = row['impression_ts']
        impression_ts = json.loads(impression_ts)
        print('\n',organization_id,' ----> ',len(impression_ts),len(click_ts))
        time.sleep(3)

    df_p.foreach(lambda x: processRowOfDF(x))

    t2 = time.time()
    print('took : ',t2-t1)


    # result = df.collect()
    # click_ts = result[0][0]
    # impression_ts = result[0][1]
    # print(type(click_ts),type(impression_ts))
