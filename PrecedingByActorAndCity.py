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

# df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("sep", ";")\
# .option('inferSchema', "false")\
# .schema(customSchema)\
# .load("/FileStore/tables/sxepgnzm1484053693268/events_hd_export_flat2.csv")

# /FileStore/tables/eyknrq101484056159506/events_hd_export_flat.csv
# /FileStore/tables/zdkspjxk1484056295645/events_hd_export_flat2.csv

# rdd = sc.textFile("C:/Users/Alexander/Desktop/spark-2.0.1-bin-hadoop2.7/bin/files/events_hd_export_flat.csv") \
# rdd = sc.textFile("/user/vako7385/events_hd_export_flat.csv") \
rdd = sc.textFile("/Users/olegeg/Desktop/Ilmenau/group_project/IlmenauGroupProject/events_temp.csv") \
    .map(lambda line: line.split(";")) \
    .filter(lambda line: line[0] != 'e_id') \
    .map(lambda line: [int(line[0]), line[1], [float(x) for x in line[2][3:-2].split(' ')] if len(line[2]) > 5 else [],
                       int(line[3]) if len(line[3]) > 0 else None, line[4], datetime.strptime(line[5], '%Y-%m-%d'),
                       line[6], line[7] if len(line[7]) > 0 else None, line[8] if len(line[8]) > 0 else None])

# rdd.collect()
df = sqlContext.createDataFrame(rdd, customSchema)
# df.registerTempTable("events")
# sqlContext.sql("select e_id as Id, concat(actor, '-', geo_name, '-', tidf.select(df.e_id.alias('e_id2'), df.geo_name.alias('geo_name2'), df.loc.alias('loc2'), df.geonamesid.alias('geonamesid2'), df.time.alias('time2'), df.time_norm.alias('time_norm2'), df.actor.alias('actor2')me_norm) as Label,  from events") \
#    .write.format('com.databricks.spark.csv').option('sep', ';').option('header', True).save('nodeCSV2')

df2 = df.filter("length(city) > 0").cache()
print df2.count()

# print(df2.take(10))

#df3 = df2
df3 = df2.filter(df.e_id == 80).cache()

actorJoin = True
counter = 1

global_counter = 2
output_column_list = []

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
time_start = time.time()

while global_counter > 0:

    current_counter_str = get_counter_str(counter)
    next_counter_str = get_counter_str(counter + 1)

    df2 = df2.withColumnRenamed('e_id' + current_counter_str, 'e_id' + next_counter_str)
    df2 = df2.withColumnRenamed('geo_name' + current_counter_str, 'geo_name' + next_counter_str)
    df2 = df2.withColumnRenamed('loc' + current_counter_str, 'loc' + next_counter_str)
    df2 = df2.withColumnRenamed('geonamesid' + current_counter_str, 'geonamesid' + next_counter_str)
    df2 = df2.withColumnRenamed('time' + current_counter_str, 'time' + next_counter_str)
    df2 = df2.withColumnRenamed('time_norm' + current_counter_str, 'time_norm' + next_counter_str)
    df2 = df2.withColumnRenamed('actor' + current_counter_str, 'actor' + next_counter_str)
    df2 = df2.withColumnRenamed('city' + current_counter_str, 'city' + next_counter_str)
    df2 = df2.withColumnRenamed('country' + current_counter_str, 'country' + next_counter_str)

    if actorJoin:

        df3 = df3.join(df2, (df3['actor' + current_counter_str] == df2['actor' + next_counter_str])
                       & (df3['time_norm' + current_counter_str] > df2['time_norm' + next_counter_str])) \
            .orderBy('time_norm' + next_counter_str)

        if counter == 1:
            nodes_df = nodes_df.union(df3.select(df3['e_id' + current_counter_str].alias('Id'),
                                      df3['actor' + current_counter_str].alias('Label'),
                                      df3['city' + current_counter_str].alias('City'))).distinct()

        nodes_df = nodes_df.union(df3.select(df3['e_id' + next_counter_str].alias('Id'),
                                             (concat(df3['actor' + next_counter_str], lit(' - '),
                                                     df3['geo_name' + next_counter_str], lit(' - '),
                                                     df3['time_norm' + next_counter_str])).alias('Label'),
                                                     df3['city' + current_counter_str].alias('City'))).distinct()

        edges_df = edges_df.union(df3.select(df3['e_id' + current_counter_str].alias('Source'),
                                  df3['e_id' + next_counter_str].alias('Target'),
                                  df3['actor' + current_counter_str].alias('Label'))).distinct()

        actorJoin = False

    else:

        df3 = df3.join(df2, (df3['time_norm' + current_counter_str] > df2['time_norm' + next_counter_str])
          & (df3['city' + current_counter_str] == df2['city' + next_counter_str])) \
            .orderBy('time_norm' + next_counter_str).cache()
        # df3 = df3.withColumn('gps_diff' + current_counter_str,
        #                      gps_diff_udf(col('loc' + current_counter_str), col('loc' + next_counter_str)))
        # df3 = df3.filter((df3['gps_diff' + current_counter_str] < 300))
                         # & (df3['time_norm' + next_counter_str] < df3['time_norm' + current_counter_str])
                         # & (df3['actor' + current_counter_str] != df3['actor' + next_counter_str])

        nodes_df = nodes_df.union(df3.select(df3['e_id' + next_counter_str].alias('Id'),
                                             (concat(df3['actor' + next_counter_str], lit(' - '),
                                                     df3['geo_name' + next_counter_str], lit(' - '),
                                                     df3['time_norm' + next_counter_str])).alias('Label'),
                                                     df3['city' + current_counter_str].alias('City'))).distinct()

        edges_df = edges_df.union(df3.select(df3['e_id' + current_counter_str].alias('Source'),
                                  df3['e_id' + next_counter_str].alias('Target'),
                                  concat(lit('Location')).alias('Label'))).distinct()

        df3.unpersist()

        actorJoin = True

    #output_column_list.append('e_id' + current_counter_str)
    #output_column_list.append('geo_name' + current_counter_str)
    #output_column_list.append('time_norm' + current_counter_str)
    #output_column_list.append('actor' + current_counter_str)

    counter += 1
    global_counter -= 1

time_end = time.time()
# df3.repartition(1).select(output_column_list).write.format('com.databricks.spark.csv')\
# .option('sep', ';').option('header', True).option("dateFormat", "YYYY-MM-DD").save('/user/vako7385/PrecedingEventsByActor2_Remote1')

#df3.repartition(1).select(output_column_list).write.format('com.databricks.spark.csv')\
#    .option('sep', ';').option('header', True).save('PrecedingEventsByActor2')

#nodes_df.registerTempTable('nodes')
#nodes_df_formatted = sqlContext.sql('select Id, Label, ')

# nodes_df.repartition(1).write.format('com.databricks.spark.csv') \
#     .option('sep', ';').option('header', True).save('/user/vako7385/PrecedingEventsByActor2_Nodes')

# edges_df.repartition(1).write.format('com.databricks.spark.csv') \
#     .option('sep', ';').option('header', True).save('/user/vako7385/PrecedingEventsByActor2_Edges')

nodes_df.repartition(1).write.format('com.databricks.spark.csv') \
   .option('sep', ';').option('header', True) \
   .save('/Users/olegeg/Desktop/Ilmenau/group_project/IlmenauGroupProject/PrecedingByActorAndCity_Nodes')

edges_df.repartition(1).write.format('com.databricks.spark.csv') \
   .option('sep', ';').option('header', True) \
   .save('/Users/olegeg/Desktop/Ilmenau/group_project/IlmenauGroupProject/PrecedingByActorAndCity_Edges')

print "time of execution ", time_end - time_start, " seconds"

