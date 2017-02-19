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

rdd = sc.textFile("/user/olga6753/events_with_city_and_country.csv") \
    .map(lambda line: line.split(";")) \
    .filter(lambda line: line[0] != 'e_id') \
    .map(lambda line: [int(line[0]), line[1], [float(x) for x in line[2][3:-2].split(' ')] if len(line[2]) > 5 else [],
                       int(line[3]) if len(line[3]) > 0 else None, line[4], datetime.strptime(line[5], '%Y-%m-%d'),
                       line[6], line[7] if len(line[7]) > 0 else None, line[8] if len(line[8]) > 0 else None])

df = sqlContext.createDataFrame(rdd, customSchema)

# !!! CHANGE THESE VALUES TO VARY THE SEARCHING LOCATION
end_date = '1943-11-01'
actor = 'Willis Augustus Lee'
start_date = '1920-01-01'
# !!! ----

start_time = time.time()
df = df.filter("length(country) > 0")
df = df.filter(df.actor == actor)
df = df.filter(df.time_norm >= start_date)
if len(end_date) == 10:
  df = df.filter(df.time_norm <= end_date)

df = df.orderBy("time_norm").repartition(1)
df.select('e_id', 'actor', 'geo_name', 'time_norm', 'country').write.format('com.databricks.spark.csv') \
    .option('sep', ';').option('header', True).save('/user/olga6753/ActorsCountries')

print("Time Interval execution time: ", (time.time() - start_time))


