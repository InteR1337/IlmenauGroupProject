from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime
from math import *
import time

conf = SparkConf().set("spark.sql.crossJoin.enabled", True)
sc = SparkContext("local", "Simple App", conf=conf)
sqlContext = SQLContext(sc)

def gps_diff(gps1, gps2):
    lat1 = gps1[0] if len(gps1) > 0 else 0
    lat2 = gps2[0] if len(gps2) > 0 else 0
    lon1 = gps1[1] if len(gps1) > 0 else 0
    lon2 = gps2[1] if len(gps2) > 0 else 0

    R = 6371
    dLat = radians(lat2 - lat1)
    dLon = radians(lon2 - lon1)
    lat1 = radians(lat1)
    lat2 = radians(lat2)

    a = sin(dLat / 2) * sin(dLat / 2) + sin(dLon / 2) * sin(dLon / 2) * cos(lat1) * cos(lat2)
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    d = R * c

    return d

def get_counter_str(counter):
    return str(counter) if counter > 1 else ''

gps_diff_udf = udf(gps_diff, FloatType())

customSchema = StructType([
    StructField("e_id", IntegerType()),
    StructField("geo_name", StringType()),
    StructField("loc", ArrayType(FloatType())),
    StructField("geonamesid", IntegerType(), True),
    StructField("time", StringType()),
    StructField("time_norm", DateType()),
    StructField("actor", StringType()),
    StructField("city", StringType()),
    StructField("country", StringType())])

start_time = time.time()
rdd = sc.textFile("/user/olga6753/events_with_city_and_country.csv") \
    .map(lambda line: line.split(";")) \
    .filter(lambda line: line[0] != 'e_id') \
    .map(lambda line: [int(line[0]), line[1], [float(x) for x in line[2][3:-2].split(' ')] if len(line[2]) > 5 else [],
                       int(line[3]) if len(line[3]) > 0 else None, line[4], datetime.strptime(line[5], '%Y-%m-%d'),
                       line[6], line[7] if len(line[7]) > 0 else None, line[8] if len(line[8]) > 0 else None])

df = sqlContext.createDataFrame(rdd, customSchema)
output_column_list = []

# !!! CHANGE THESE VALUES TO VARY THE SEARCHING LOCATION
search_depth = 2
root_event_id = 999
date_threshold = 365 * 200
# !!! ----

df = df.where(length(col("city")) > 0).cache()
df2 = df.filter(df.e_id == root_event_id)

nodes_df_schema = StructType([
    StructField("Id", StringType(), True),
    StructField("Label", StringType(), True),
    StructField("City", StringType(), True)
])

edges_df_schema = StructType([
    StructField("Source", StringType(), True),
    StructField("Target", StringType(), True),
    StructField("Label", StringType(), True)
])

nodes_df = sqlContext.createDataFrame([], nodes_df_schema)
edges_df = sqlContext.createDataFrame([], edges_df_schema)

actorJoin = True
step_counter = 1
while search_depth > 0:

    current_counter_str = get_counter_str(step_counter)
    next_counter_str = get_counter_str(step_counter + 1)

    df = df.withColumnRenamed('e_id' + current_counter_str, 'e_id' + next_counter_str)
    df = df.withColumnRenamed('geo_name' + current_counter_str, 'geo_name' + next_counter_str)
    df = df.withColumnRenamed('loc' + current_counter_str, 'loc' + next_counter_str)
    df = df.withColumnRenamed('geonamesid' + current_counter_str, 'geonamesid' + next_counter_str)
    df = df.withColumnRenamed('time' + current_counter_str, 'time' + next_counter_str)
    df = df.withColumnRenamed('time_norm' + current_counter_str, 'time_norm' + next_counter_str)
    df = df.withColumnRenamed('actor' + current_counter_str, 'actor' + next_counter_str)
    df = df.withColumnRenamed('city' + current_counter_str, 'city' + next_counter_str)
    df = df.withColumnRenamed('country' + current_counter_str, 'country' + next_counter_str)

    if step_counter == 1:
      output_column_list.append('e_id' + current_counter_str)
      output_column_list.append('time_norm' + current_counter_str)
      output_column_list.append('actor' + current_counter_str)

    output_column_list.append('e_id' + next_counter_str)
    output_column_list.append('time_norm' + next_counter_str)
    output_column_list.append('actor' + next_counter_str)

    if actorJoin:

        df2 = df2.join(df, (df2['actor' + current_counter_str] == df['actor' + next_counter_str])
                       & (df['time_norm' + next_counter_str] < df2['time_norm' + current_counter_str])) \
        .orderBy('time_norm' + next_counter_str)

        if step_counter == 1:
            nodes_df = nodes_df.union(df2.select(df2['e_id' + current_counter_str].alias('Id'),
                                      (concat(df2['actor' + current_counter_str], lit(' - '),
                                              df2['geo_name' + current_counter_str], lit(' - '),
                                              df2['time_norm' + current_counter_str])).alias('Label'),
                                  df2['city' + current_counter_str].alias('City')))

        if date_threshold > 0:
          df2 = df2.withColumn('date_diff' + current_counter_str, datediff(df2['time_norm' + current_counter_str], df2['time_norm' + next_counter_str]))
          df2 = df2.filter(df2['date_diff' + current_counter_str] < date_threshold)
          output_column_list.append('date_diff' + current_counter_str)

        #nodes_df = nodes_df.union(df2.select(df2['e_id' + next_counter_str].alias('Id'),
        #                                     (concat(df2['actor' + next_counter_str], lit(' - '),
         #                                            df2['geo_name' + next_counter_str], lit(' - '),
         #                                            df2['time_norm' + next_counter_str])).alias('Label'),
         #                          df2['city' + next_counter_str].alias('City'))).distinct()

        #dges_df = edges_df.union(df2.select(df2['e_id' + current_counter_str].alias('Source'),
         #                         df2['e_id' + next_counter_str].alias('Target'),
        #                          df2['actor' + current_counter_str].alias('Label'))).distinct()

        actorJoin = False

    else:
        df2 = df2.join(df, (df['time_norm' + next_counter_str] < df2['time_norm' + current_counter_str])
                       & (df['city' + next_counter_str] == df2['city' + current_counter_str])) \
            .orderBy('time_norm' + next_counter_str).cache()

        if date_threshold > 0:
          df2 = df2.withColumn('date_diff' + current_counter_str, datediff(df2['time_norm' + current_counter_str], df2['time_norm' + next_counter_str]))
          df2 = df2.filter(df2['date_diff' + current_counter_str] < date_threshold)
          output_column_list.append('date_diff' + current_counter_str)

        #nodes_df = nodes_df.union(df2.select(df2['e_id' + next_counter_str].alias('Id'),
         #                                    (concat(df2['actor' + next_counter_str], lit(' - '),
         #                                            df2['geo_name' + next_counter_str], lit(' - '),
        #                                             df2['time_norm' + next_counter_str])).alias('Label'),
         #                         df2['city' + next_counter_str].alias('City'))).distinct()

        #edges_df = edges_df.union(df2.select(df2['e_id' + current_counter_str].alias('Source'),
        #                          df2['e_id' + next_counter_str].alias('Target'),
        #                          concat(lit('Location')).alias('Label'))).distinct()

        df2.unpersist()

        output_column_list.append('city' + current_counter_str)
        actorJoin = True

    step_counter += 1
    search_depth -= 1

df2.repartition(1).select(output_column_list).write.format('com.databricks.spark.csv')\
  .option('sep', ';').option('header', True).option("dateFormat", "yyyy-MM-dd") \
  .save('/user/olga6753/PrecedingByActorAndCity')

print("Preceding Events By Actor execution time: ", (time.time() - start_time))

# nodes_df.repartition(1).write.format('com.databricks.spark.csv') \
#    .option('sep', ';').option('header', True) \
#    .save('/Users/olegeg/Desktop/Ilmenau/group_project/IlmenauGroupProject/PrecedingByActorAndCity_Nodes')

# edges_df.repartition(1).write.format('com.databricks.spark.csv') \
#    .option('sep', ';').option('header', True) \
#    .save('/Users/olegeg/Desktop/Ilmenau/group_project/IlmenauGroupProject/PrecedingByActorAndCity_Edges')

