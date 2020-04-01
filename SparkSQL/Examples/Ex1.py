from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import os
import json
import time
import yaml
from SparkSQL.Examples import SparkContextCreator,SparkSessionCreator

if __name__ == "__main__":
    os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3.6"

    NUM_CORE = 6

    sc, conf = SparkContextCreator.SparkContextCreator(NUM_CORE=NUM_CORE)
    spark = SparkSessionCreator.SparkSessionCreator(NUM_CORE,conf)


    t1=time.time()

    query = 'select organization_id,click_ts,impression_ts  from organization_listing_report_ts limit 30'

    my_dir = os.path.expanduser('/home/zevik/PycharmProjects/pyspark-basic-examples/SparkSQL/db_configs.yaml')
    conn_dict = yaml.load(open(my_dir))
    DB_NAME = conn_dict['test']['dbname']
    USER_NAME = conn_dict['test']['user']
    PASSWORD = conn_dict['test']['password']
    HOST = host=conn_dict['test']['host']
    CONNECTION_URL = "jdbc:postgresql://" + str(HOST) + ":5432/" +  str(DB_NAME)

    df = spark.read \
        .format("jdbc") \
        .option("url", CONNECTION_URL) \
        .option("query", query) \
        .option("user", USER_NAME) \
        .option("password", PASSWORD) \
        .option("driver",'org.postgresql.Driver')\
        .option("numPartitions",1)\
        .load()

    df.printSchema()
    print(type(df))
    df.show()

    df_p = spark.createDataFrame(sc.parallelize(df.collect(),numSlices=NUM_CORE)) # Parallelize Gotten DF to make Spark App Parallel with n_cores Thread
    print('number of partiotions is : ',df_p.rdd.getNumPartitions())


    # ----------- working with DataFrame (High Level RDD API from Spark) --------
    def processRowOfDF(row,demo=False):
        organization_id = row['organization_id']
        click_ts = row['click_ts']
        click_ts = json.loads(click_ts)
        impression_ts = row['impression_ts']
        impression_ts = json.loads(impression_ts)
        print('\n\n',organization_id,' ----> ',len(impression_ts),len(click_ts))
        if demo:
            time.sleep(2)

    df_p.foreach(lambda x: processRowOfDF(x,demo=True)) # Set demo to True to debug the magic of parallel processing, set false to parallel compute regularly

    t2 = time.time()
    print('took : ',t2-t1)


    # result = df.collect()
    # click_ts = result[0][0]
    # impression_ts = result[0][1]
    # print(type(click_ts),type(impression_ts))
