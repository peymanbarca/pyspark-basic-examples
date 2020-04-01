from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import os
import json
import time
import yaml
from SparkSQL.Examples import SparkContextCreator,SparkSessionCreator
import psycopg2
import numpy as np
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType,FloatType,LongType,DecimalType
import json
from geopy.distance import geodesic


if __name__ == "__main__":

    from pyspark.sql.functions import lit
    os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3.6"

    NUM_CORE = 4

    sc, conf = SparkContextCreator.SparkContextCreator(NUM_CORE=NUM_CORE)
    spark = SparkSessionCreator.SparkSessionCreator(NUM_CORE,conf)

    print('\n\n----------------------------------------------------------------------')
    print(' --------------------------- Start Of Application --------------------')

    t1=time.time()


    my_dir = os.path.expanduser('/home/zevik/PycharmProjects/pyspark-basic-examples/SparkSQL/db_configs.yaml')
    conn_dict = yaml.load(open(my_dir))
    DB_NAME = conn_dict['test']['dbname']
    USER_NAME = conn_dict['test']['user']
    PASSWORD = conn_dict['test']['password']
    HOST = host=conn_dict['test']['host']
    CONNECTION_URL = "jdbc:postgresql://" + str(HOST) + ":5432/" +  str(DB_NAME)

    db_conn = psycopg2.connect(host = HOST,database=DB_NAME,user=USER_NAME,password=PASSWORD )
    db_cursor = db_conn.cursor()


    # -------------------- fetch target listing data ----------------------

    target_listing_id = 994954

    fetch_target_query = 'select id,city_id,region_id,neighbourhood_id,area_id,EXTRACT(EPOCH FROM listing_date) * 1000 as listing_date,floor_area,listing_type_id,price,' \
                         'longitude::numeric,latitude::numeric ' \
                         'from listing where id={}'.format(target_listing_id)



    db_cursor.execute(fetch_target_query)
    fetch_target_data = db_cursor.fetchall()
    target_city_id = int(fetch_target_data[0][1])
    target_region_id = int(fetch_target_data[0][2])
    target_neighbourhood_id = int(fetch_target_data[0][3])
    target_area_id = int(fetch_target_data[0][4])
    target_listing_date = int(fetch_target_data[0][5])
    target_floor_area = fetch_target_data[0][6]
    target_listing_type_id = int(fetch_target_data[0][7])
    target_price = int(fetch_target_data[0][8])
    target_longitude = fetch_target_data[0][9]
    target_latitude = fetch_target_data[0][10]
    target_diff = None

    target_vector = [(target_listing_id,target_city_id,target_region_id,target_neighbourhood_id,target_area_id,target_listing_date,target_floor_area,target_listing_type_id,
                      target_price,target_longitude,target_latitude)]

    schema = StructType([
        StructField('id', LongType(), True),StructField('city_id', LongType(), True),StructField('region_id', LongType(), True),
        StructField('neighbourhood_id', LongType(), True),StructField('area_id', IntegerType(), True),
        StructField('listing_date', LongType(), True),StructField('floor_area', FloatType(), True),
        StructField('listing_type_id', IntegerType(), True),StructField('price', LongType(), True),
        StructField('longitude', DecimalType(18,15), True), StructField('latitude', DecimalType(18,15), True)
    ])


    target_df = spark.createDataFrame(target_vector,schema=schema)
    target_df = target_df.withColumn("total_delta", lit(None))
    target_df.show()



    # -------------------- fetch related listing data in the same region ----------------------
    sample_size = 2000
    fetch_related_query = 'select id,city_id,region_id,neighbourhood_id,area_id,(EXTRACT(EPOCH FROM listing_date) * 1000)::bigint as listing_date,floor_area,listing_type_id,price,' \
                          'longitude::numeric,latitude::numeric ' \
                         'from listing where id!={} and city_id={} and region_id={} and listing_type_id={} order by listing_date desc limit 1000'\
                            .format(target_listing_id,target_city_id,target_region_id,target_listing_type_id)

    db_cursor.execute(fetch_related_query)
    fetch_related_data = db_cursor.fetchall()

    related_rdd = sc.parallelize(fetch_related_data,numSlices=NUM_CORE)
    related_df = related_rdd.map(lambda x: list(x)).toDF(schema=schema)

    related_df = related_df.withColumn("total_delta",lit(None))
    related_df.show(3)



    # ------------------ Content Based Filtering Recommendation System ----------------------------
    sorted_distances = []

    def contentBasedRecommenderScore(related_df_elem,target_vector):

        id = related_df_elem['id']
        neighbourhood_id = related_df_elem['neighbourhood_id']
        area_id = related_df_elem['area_id']
        listing_date = related_df_elem['listing_date']
        floor_area = related_df_elem['floor_area']
        price = related_df_elem['price']
        longitude = float(related_df_elem['longitude'])
        latitude = float(related_df_elem['latitude'])


        def compute_distance():



            try:

                target_neighbourhood_id = target_vector[0][3]
                target_area_id = target_vector[0][4]
                target_listing_date = target_vector[0][5]
                target_floor_area = target_vector[0][6]
                target_price = target_vector[0][8]
                target_longitude = float(target_vector[0][9])
                target_latitude = float(target_vector[0][10])

                delta_listing_date = np.sqrt( ( np.abs(target_listing_date-listing_date)))
                delta_floor_area = np.sqrt( np.abs(target_floor_area-floor_area) )
                delta_price = np.sqrt( np.abs(target_price - price) )

                distance = geodesic((target_latitude,target_longitude), (latitude,longitude)).meters

                total_delta = np.average([delta_listing_date,delta_floor_area,delta_price,distance],weights=[2/10,2/10,2/10,4/10])


                # print(id,total_delta,distance)
                distance_detail = {'id':id,'total_distance': round(total_delta,3)}
                with open("Output.txt", "a") as text_file:
                    text_file.write(json.dumps(distance_detail)+'\n')

            except Exception as e:
                pass

        compute_distance()





    total_df = target_df.union(related_df)

    with open("Output.txt", "w") as text_file:
        text_file.write("")

    # --------- compute Score of Similarity for Content based Recommendation System -------
    total_df.foreach(lambda x: contentBasedRecommenderScore(x,target_vector))

    # -------- Examine Score Results and Sorting -------
    with open("Output.txt") as f:
        scores = f.readlines()
    scores = [json.loads(x.strip()) for x in scores]
    scores_rdd = sc.parallelize(scores, numSlices=1)

    DEBUG_MODE = False

    if DEBUG_MODE:
        print(' Scores RDD : -----------')
        scores_rdd.foreach(lambda x:print(x['id'],x['total_distance']))

    # sorting by distance
    scores_rdd_Sorted = scores_rdd.sortBy(lambda x: x['total_distance'])

    if DEBUG_MODE:
        print('Sorted Scores RDD : -----------')
        scores_rdd_Sorted.foreach(lambda x: print(x['id'], x['total_distance']))

    print('Best Items Are : ')
    best=scores_rdd_Sorted.collect()[1:5]
    print(best)




    t2 = time.time()
    print('took : ',t2-t1)