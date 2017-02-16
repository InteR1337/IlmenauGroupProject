from pyspark import SparkContext
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
  dLat = radians(lat2-lat1)
  dLon = radians(lon2-lon1)
  lat1 = radians(lat1)
  lat2 = radians(lat2)

  a = sin(dLat/2) * sin(dLat/2) + sin(dLon/2) * sin(dLon/2) * cos(lat1) * cos(lat2)
  c = 2 * atan2(sqrt(a), sqrt(1-a))
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
    StructField("actor", StringType())])

rdd = sc.textFile("/Users/olegeg/Desktop/Ilmenau/group_project/IlmenauGroupProject/events_hd_export_flat.csv") \
    .map(lambda line: line.split(";")) \
    .filter(lambda line: line[0] != 'e_id') \
    .map(lambda line: [int(line[0]), line[1], [float(x) for x in line[2][3:-2].split(' ')] if len(line[2]) > 5 else [],
      int(line[3]) if len(line[3]) > 0 else None, line[4], datetime.strptime(line[5], '%Y-%m-%d'), line[6]])

df = sqlContext.createDataFrame(rdd, customSchema)
df2 = df.filter(size(df.loc) > 0).cache()
df3 = df2.orderBy(df2.time_norm.desc()).dropDuplicates(['actor']).cache()
df.unpersist()
# for elem in df3.collect():
#     print elem

actorJoin = True
counter = 1
global_counter = 6
output_column_list = []
time_start = time.time()

while global_counter > 0:

    current_counter_str = get_counter_str(counter)
    next_counter_str = get_counter_str(counter + 1)
    print "step ", current_counter_str

    df2 = df2.withColumnRenamed('e_id' + current_counter_str, 'e_id' + next_counter_str)
    df2 = df2.withColumnRenamed('geo_name' + current_counter_str, 'geo_name' + next_counter_str)
    df2 = df2.withColumnRenamed('loc' + current_counter_str, 'loc' + next_counter_str)
    df2 = df2.withColumnRenamed('geonamesid' + current_counter_str, 'geonamesid' + next_counter_str)
    df2 = df2.withColumnRenamed('time' + current_counter_str, 'time' + next_counter_str)
    df2 = df2.withColumnRenamed('time_norm' + current_counter_str, 'time_norm' + next_counter_str)
    df2 = df2.withColumnRenamed('actor' + current_counter_str, 'actor' + next_counter_str)

    if actorJoin:

        df3 = df3.join(df2, (df3['actor' + current_counter_str] == df2['actor' + next_counter_str])
                       & (df3['time_norm' + current_counter_str] > df2['time_norm' + next_counter_str])) \
            .orderBy('time_norm' + next_counter_str)
        actorJoin = False

    else:

        df3 = df3.join(df2, df3['time_norm' + current_counter_str] > df2['time_norm' + next_counter_str]) \
            .orderBy('time_norm' + next_counter_str).cache()
        df3 = df3.withColumn('gps_diff' + current_counter_str,
                             gps_diff_udf(col('loc' + current_counter_str), col('loc' + next_counter_str)))
        df3 = df3.filter((df3['gps_diff' + current_counter_str] < 300)
                         # & (df3['time_norm' + next_counter_str] < df3['time_norm' + current_counter_str])
                         # & (df3['actor' + current_counter_str] != df3['actor' + next_counter_str])
                         )

        df3.unpersist()
        actorJoin = True

    counter += 1
    global_counter -= 1

time_end = time.time()

df = df3.groupby('actor').count().orderBy('count', ascending=False)
# print df

df.repartition(1).select(df['actor'], df['count'].alias('related_events')).write.format('com.databricks.spark.csv')\
.option('sep', ';').option('header', True) \
.save('/Users/olegeg/Desktop/Ilmenau/group_project/IlmenauGroupProject/MostRelatedEvents')

#df3.repartition(1).select(output_column_list).write.format('com.databricks.spark.csv')\
#    .option('sep', ';').option('header', True).save('PrecedingEventsByActor2')

#nodes_df.registerTempTable('nodes')
#nodes_df_formatted = sqlContext.sql('select Id, Label, ')

# nodes_df.repartition(1).write.format('com.databricks.spark.csv') \
#     .option('sep', ';').option('header', True).save('/user/vako7385/PrecedingEventsByActor2_Nodes')

# edges_df.repartition(1).write.format('com.databricks.spark.csv') \
#     .option('sep', ';').option('header', True).save('/user/vako7385/PrecedingEventsByActor2_Edges')

# nodes_df.repartition(1).write.format('com.databricks.spark.csv') \
#    .option('sep', ';').option('header', True) \
#    .save('/Users/olegeg/Desktop/Ilmenau/group_project/IlmenauGroupProject/PrecedingEventsByActor2_Nodes')

# edges_df.repartition(1).write.format('com.databricks.spark.csv') \
#    .option('sep', ';').option('header', True) \
#    .save('/Users/olegeg/Desktop/Ilmenau/group_project/IlmenauGroupProject/PrecedingEventsByActor2_Edges')

print "time of execution ", time_end - time_start, " seconds"
