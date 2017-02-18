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

gps_diff_udf=udf(gps_diff, FloatType())
  
customSchema = StructType([
    StructField("e_id", IntegerType()),
    StructField("geo_name", StringType()),
    StructField("loc", ArrayType(FloatType())),
    StructField("geonamesid", IntegerType(), True),
    StructField("time", StringType()),
    StructField("time_norm", DateType()),
    StructField("actor", StringType())])

start_time = time.time()
rdd = sc.textFile("C:/Users/Alexander/Desktop/spark-2.0.1-bin-hadoop2.7/bin/files/events_hd_export_flat.csv") \
    .map(lambda line: line.split(";")) \
    .filter(lambda line: line[0] != 'e_id') \
    .map(lambda line: [int(line[0]), line[1], [float(x) for x in line[2][3:-2].split(' ')] if len(line[2]) > 5 else [],
                       int(line[3]) if len(line[3]) > 0 else None, line[4], datetime.strptime(line[5], '%Y-%m-%d'),
                       line[6]])

df = sqlContext.createDataFrame(rdd, customSchema)

# !!! CHANGE THESE VALUES TO VARY THE SEARCHING LOCATION 
latitude = 42.223 
longitude = -121.7775
threshold = 500
time_start = '1800-01-01'
actor = 'Abraham Lincoln'
# !!! ----

df = df.filter(size(df.loc) > 0).cache()
df = df.withColumn('gps_init', array(lit(latitude), lit(longitude)))
df = df.withColumn('gps_diff', gps_diff_udf(df.loc, df.gps_init))
df = df.filter((df.gps_diff < threshold) & (df.actor == actor))

if len(time_start) == 10:
  df = df.filter(df.time_norm >= time_start)

df = df.orderBy("time_norm").repartition(1)
df.select('e_id', 'actor', 'time_norm', 'gps_diff').write.format('com.databricks.spark.csv') \
    .option('sep', ';').option('header', True).save('ExactLocationAndActor')

print("Exact Location And Actor execution time: ", (time.time() - start_time))

#df.select(df['e_id'].alias('Id'), (concat(df['geo_name'], lit(' - '), df['time_norm'])).alias('Label')).write.format('com.databricks.spark.csv').option('sep', ';').option('header', True).save('ExactLocationAndActor_Nodes')
