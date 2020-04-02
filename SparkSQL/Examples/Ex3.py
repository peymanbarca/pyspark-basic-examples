from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from math import sin, cos, sqrt, atan2, radians
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
from pyspark.sql import SQLContext


if __name__ == "__main__":

    from pyspark.sql.functions import lit
    os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3.6"

    NUM_CORE = 4

    sc, conf = SparkContextCreator.SparkContextCreator(NUM_CORE=NUM_CORE)
    spark = SparkSessionCreator.SparkSessionCreator(NUM_CORE,conf)

    sqlContext = SQLContext(sc)

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


    # -------------------- fetch Sample listing data ----------------------
    sample_size = 10000
    region_id = 2301021601
    fetch_related_query = 'select id,city_id,region_id,neighbourhood_id,area_id,(EXTRACT(EPOCH FROM listing_date) * 1000)::bigint as listing_date,floor_area,listing_type_id,price,' \
                          'longitude::numeric,latitude::numeric ' \
                         'from listing where region_id={} and listing_type_id=1  order by listing_date desc limit {}'\
                            .format(region_id,sample_size)

    db_cursor.execute(fetch_related_query)
    fetch_related_data = db_cursor.fetchall()

    schema = StructType([
        StructField('id', LongType(), True),StructField('city_id', LongType(), True),StructField('region_id', LongType(), True),
        StructField('neighbourhood_id', LongType(), True),StructField('area_id', IntegerType(), True),
        StructField('listing_date', LongType(), True),StructField('floor_area', FloatType(), True),
        StructField('listing_type_id', IntegerType(), True),StructField('price', LongType(), True),
        StructField('longitude', DecimalType(18,15), True), StructField('latitude', DecimalType(18,15), True)
    ])

    sample_listing_rdd = sc.parallelize(fetch_related_data,numSlices=NUM_CORE)
    sample_listing_df = sample_listing_rdd.map(lambda x: list(x)).toDF(schema=schema) # explicit schema

    num_partiotioned = sample_listing_df.rdd.getNumPartitions()
    print('Number of Partitions Are : ',num_partiotioned,'\n\n' , 'Total Rows Are : ',sample_listing_df.count(),'\n\n')

    sample_listing_df.show(3)

    # ------------------ Register Data as tmp table to perform SQL query in memory ----------------------------
    sample_listing_df.registerTempTable("sample_listing_region")

    filterd_df = sqlContext.sql("select * from sample_listing_region where neighbourhood_id=230102160126 and floor_area>200")
    filterd_df.show(3)


    # ------------------ Content Based Filtering Recommendation System for each listing ----------------------------
    t1 = time.time()




    def contentBasedRecommenderScore(related_df_elem,related_data):

        start_time = time.time()

        id = related_df_elem['id']
        neighbourhood_id = related_df_elem['neighbourhood_id']
        area_id = related_df_elem['area_id']
        listing_date = related_df_elem['listing_date']
        floor_area = related_df_elem['floor_area']
        price = related_df_elem['price']
        longitude = float(related_df_elem['longitude'])
        latitude = float(related_df_elem['latitude'])


        def compute_distance():

            distances = []
            related_data_sample = filter(lambda ROW: ROW['floor_area'] < 1.3*floor_area and ROW['floor_area'] > 0.7*floor_area, related_data)

            for related_listing_data in related_data_sample:
                target_id = related_listing_data['id']
                target_listing_date = related_listing_data['listing_date']
                target_floor_area = related_listing_data['floor_area']
                target_price = related_listing_data['price']
                target_longitude = float(related_listing_data['longitude'])
                target_latitude = float(related_listing_data['latitude'])

                try:
                    delta_listing_date = np.sqrt((np.abs(target_listing_date - listing_date)))
                    delta_floor_area = np.sqrt(np.abs(target_floor_area - floor_area))
                    delta_price = np.sqrt(np.abs(target_price - price))

                    R = 6373.0

                    lat1 = radians(target_latitude)
                    lon1 = radians(target_longitude)
                    lat2 = radians(latitude)
                    lon2 = radians(longitude)

                    dlon = lon2 - lon1
                    dlat = lat2 - lat1

                    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
                    c = 2 * atan2(sqrt(a), sqrt(1 - a))

                    distance = R * c
                    total_delta = np.average([delta_listing_date, delta_floor_area, delta_price,distance],
                                             weights=[2 / 10, 3 / 10,3 / 10, 2 / 10])

                    distances.append({'listing_id':target_id,'total_distance': round(total_delta,3)})

                except Exception as e:
                    continue

            newlist = sorted(distances, key=lambda k: k['total_distance'])
            MIN_DISTANCE_DICT = newlist[1:5] if len(newlist)>4 else None
            end_time = time.time()

            result = {'target_listing_id' : id , 'content' : MIN_DISTANCE_DICT }

            with open("OutputTotal.txt", "a") as text_file:
                text_file.write(json.dumps(result) + '\n')

            print(target_id, ' took : ',end_time-start_time)

        compute_distance()



    # --------- compute Score of Similarity for Content based Recommendation System -------
    with open("OutputTotal.txt", "w") as text_file:
        text_file.write("")

    related_data = sample_listing_df.collect()

    # Remember that SparkContext can only be used on the driver, not in code that it run on workers
    sample_listing_df.foreach(lambda x: contentBasedRecommenderScore(x,related_data)) # going from driver to worker nodes