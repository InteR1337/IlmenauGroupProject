from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime
from math import *

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

gps_diff_udf=udf(gps_diff, FloatType())
  
customSchema = StructType([
    StructField("e_id", IntegerType()),
    StructField("geo_name", StringType()),
    StructField("loc", ArrayType(FloatType())),
    StructField("geonamesid", IntegerType(), True),
    StructField("time", StringType()),
    StructField("time_norm", DateType()),
    StructField("actor", StringType())])

#df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("sep", ";")\
#.option('inferSchema', "false")\
#.schema(customSchema)\
#.load("/FileStore/tables/sxepgnzm1484053693268/events_hd_export_flat2.csv")

#/FileStore/tables/eyknrq101484056159506/events_hd_export_flat.csv
#/FileStore/tables/zdkspjxk1484056295645/events_hd_export_flat2.csv

rdd = sc.textFile("/user/vako7385/events_hd_export_flat.csv") \
    .map(lambda line: line.split(";")) \
    .filter(lambda line: line[0] != 'e_id') \
    .map(lambda line: [int(line[0]), line[1], [float(x) for x in line[2][3:-2].split(' ')] if len(line[2]) > 5 else [], int(line[3]) if len(line[3]) > 0 else None, line[4], datetime.strptime(line[5], '%Y-%m-%d'), line[6]])

#rdd.collect()  
df = sqlContext.createDataFrame(rdd, customSchema)
#df.registerTempTable("events")
#sqlContext.sql("select e_id as Id, concat(actor, '-', geo_name, '-', tidf.select(df.e_id.alias('e_id2'), df.geo_name.alias('geo_name2'), df.loc.alias('loc2'), df.geonamesid.alias('geonamesid2'), df.time.alias('time2'), df.time_norm.alias('time_norm2'), df.actor.alias('actor2')me_norm) as Label,  from events") \
#    .write.format('com.databricks.spark.csv').option('sep', ';').option('header', True).save('nodeCSV2')

df2 = df.filter(size(df.loc) > 0).cache()

df3 = df2.filter(df.e_id == 1781).cache()

df2 = df2.withColumnRenamed('e_id', 'e_id2')
df2 = df2.withColumnRenamed('geo_name', 'geo_name2')
df2 = df2.withColumnRenamed('loc', 'loc2')
df2 = df2.withColumnRenamed('geonamesid', 'geonamesid2')
df2 = df2.withColumnRenamed('time', 'time2')
df2 = df2.withColumnRenamed('time_norm', 'time_norm2')
df2 = df2.withColumnRenamed('actor', 'actor2')

df4 = df3.join(df2, (df3.actor == df2.actor2) & (df2.time_norm2 < df3.time_norm), 'left_outer')
df4 = df4.orderBy('time_norm2')

df2 = df2.withColumnRenamed('e_id2', 'e_id3')
df2 = df2.withColumnRenamed('geo_name2', 'geo_name3')
df2 = df2.withColumnRenamed('loc2', 'loc3')
df2 = df2.withColumnRenamed('geonamesid2', 'geonamesid3')
df2 = df2.withColumnRenamed('time2', 'time3')
df2 = df2.withColumnRenamed('time_norm2', 'time_norm3')
df2 = df2.withColumnRenamed('actor2', 'actor3')

df5 = df4.join(df2).cache()
#df5 = df5.withColumn('time_diff', abs(datediff(df5.time_norm2, df5.time_norm3)))
df5 = df5.withColumn('gps_diff', gps_diff_udf(df5.loc2, df5.loc3))
df5 = df5.filter((df5.gps_diff < 500) & (df5.time_norm3 < df5.time_norm2) & (df5.actor3 != df5.actor2)).repartition(1)

df5.select('e_id', 'actor', 'time_norm', 'e_id2', 'actor2', 'time_norm2', 'e_id3', 'actor3', 'time_norm3').write.format('com.databricks.spark.csv')\
    .option('sep', ';').option('header', True).save('/user/vako7385/PrecedingEventsByActor_Remote1')

#df2 = df2.filter((df2.gps_diff < 50)).repartition(1)

#df2.registerTempTable('events')
#sqlContext.sql("select e_id as Source from events") \
#    .write.format('com.databricks.spark.csv').option('sep', ';').option('header', True).save('edgeCSV2')


#print(df2.select('e_id', 'e_id2', 'time_diff', 'gps_diff').show())
# 'e_id2', 'geo_name2', 'time_norm2', 'actor2',
#df2.select('e_id', 'geo_name', 'time_norm', 'actor', 'gps_diff') \
#    .write.format('com.databricks.spark.csv').option('sep', ';').option('header', True).save('test2')

#df2 = df.orderBy(["actor", "time_norm"], ascending=[1, 1]).filter(col('actor') == 'Abraham Lincoln').repartition(1)
#df4.select('e_id2', 'geo_name2', 'time_norm2', 'actor2').write.format('com.databricks.spark.csv')\
#    .option('sep', ';').option('header', True).save('PrecedingEventsByActor1')
