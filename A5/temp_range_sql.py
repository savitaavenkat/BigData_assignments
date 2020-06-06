import sys
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('Weather DataFrame+Python').getOrCreate()
assert sys.version_info >= (3, 5) # make sure we have Pythonspark = SparkSession.builder.appName('example code').getOrCreate() 3.5+
assert spark.version >= '2.3' # make sure we have Spark 2.3+

# add more functions as necessary

def main(inputs, output):
        # main logic starts here
        observation_schema = types.StructType([
        types.StructField('station', types.StringType(), False),
        types.StructField('date', types.StringType(), False),
        types.StructField('observation', types.StringType(), False),
        types.StructField('value', types.IntegerType(), False),
        types.StructField('mflag', types.StringType(), False),
        types.StructField('qflag', types.StringType(), False),
        types.StructField('sflag', types.StringType(), False),
        types.StructField('obstime', types.StringType(), False),
        ])
        weather = spark.read.csv(inputs, schema=observation_schema)
        weather.createOrReplaceTempView("weather")
        weather_filtered_qflag = spark.sql("SELECT * FROM weather WHERE qflag IS NULL")
        weather_filtered_qflag.createOrReplaceTempView("weather_filtered_qflag")
        weather_tmax = spark.sql("SELECT station, date, value AS tmax FROM weather_filtered_qflag WHERE observation == 'TMAX'")
        weather_tmax.createOrReplaceTempView("weather_tmax")
        weather_tmin = spark.sql("SELECT station, date, value AS tmin FROM weather_filtered_qflag WHERE observation == 'TMIN'")
        weather_tmin.createOrReplaceTempView("weather_tmin")
        weather_max = spark.sql("SELECT weather_tmax.date, weather_tmax.station, weather_tmax.tmax, weather_tmin.tmin FROM weather_tmax JOIN weather_tmin ON weather_tmax.station=weather_tmin.station AND weather_tmax.date=weather_tmin.date")
        weather_max.createOrReplaceTempView("weather_max")
        weather_max = spark.sql("SELECT date, station, (tmax-tmin)/10 AS range FROM weather_max")
        weather_max.createOrReplaceTempView("weather_max_filtered")
        weather_range_max = spark.sql("SELECT date, MAX(range) AS range FROM weather_max_filtered GROUP BY date ORDER BY date")
        weather_range_max.createOrReplaceTempView("weather_range_max")
        weather_final = spark.sql("SELECT weather_max_filtered.date, weather_max_filtered.station, weather_range_max.range FROM weather_range_max JOIN weather_max_filtered ON weather_range_max.date=weather_max_filtered.date AND weather_range_max.range=weather_max_filtered.range ORDER BY weather_max_filtered.date, weather_max_filtered.station")
        weather_final.show(250)
        weather_final.write.csv(output, mode='overwrite')

if __name__ == '__main__':
        inputs = sys.argv[1]
        output = sys.argv[2]
        main(inputs, output)
