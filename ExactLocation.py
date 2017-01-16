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

rdd = sc.textFile("C:/Users/Alexander/Desktop/spark-2.0.1-bin-hadoop2.7/bin/files/events_hd_export_flat.csv") \
    .map(lambda line: line.split(";")) \
    .filter(lambda line: line[0] != 'e_id') \
    .map(lambda line: [int(line[0]), line[1], [float(x) for x in line[2][3:-2].split(' ')] if len(line[2]) > 5 else [], int(line[3]) if len(line[3]) > 0 else None, line[4], datetime.strptime(line[5], '%Y-%m-%d'), line[6]])

df = sqlContext.createDataFrame(rdd, customSchema)
#df.registerTempTable("events")
#sqlContext.sql("select e_id as Id, concat(actor, '-', geo_name, '-', time_norm) as Label,  from events") \
#    .write.format('com.databricks.spark.csv').option('sep', ';').option('header', True).save('nodeCSV2')

df = df.filter(size(df.loc) > 0).cache()
#df2 = df.join(df.select(df.e_id.alias('e_id2'), df.geo_name.alias('geo_name2'), df.loc.alias('loc2'), df.geonamesid.alias('geonamesid2'), df.time.alias('time2'), df.time_norm.alias('time_norm2'), df.actor.alias('actor2')))
df2 = df.withColumn('gps_init', array(lit(51.496), lit(-0.1396)))
df2 = df2.orderBy("time_norm").cache()
    
#df2 = df2.withColumn('time_diff', abs(datediff(df2.time_norm, df2.time_norm2)))
df2 = df2.withColumn('gps_diff', gps_diff_udf(df2.loc, df2.gps_init))
df2 = df2.filter((df2.gps_diff < 50) & (df2.time_norm >= '1900-01-01')).repartition(1)

df2.select('e_id', 'actor', 'time_norm', 'gps_diff').write.format('com.databricks.spark.csv') \
    .option('sep', ';').option('header', True).save('ExactLocation1')

#df2.registerTempTable('events')
#sqlContext.sql("select e_id as Source from events") \
#    .write.format('com.databricks.spark.csv').option('sep', ';').option('header', True).save('edgeCSV2')

#print(df2.select('e_id', 'e_id2', 'time_diff', 'gps_diff').show())
# 'e_id2', 'geo_name2', 'time_norm2', 'actor2',
#df2.select('e_id', 'geo_name', 'time_norm', 'actor', 'gps_diff') \
#    .write.format('com.databricks.spark.csv').option('sep', ';').option('header', True).save('test2')

#df2 = df.orderBy(["actor", "time_norm"], ascending=[1, 1]).filter(col('actor') == 'Abraham Lincoln').repartition(1)
#df2.select('e_id', 'geo_name', 'time_norm', 'actor').write.format('com.databricks.spark.csv').option('sep', ';').option('header', True).save('test')
