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
        weather_filtered_qflag = weather.filter(weather.qflag.isNull())
        weather_filtered_qflag.cache()
        weather_tmax = weather_filtered_qflag.filter(weather_filtered_qflag.observation == 'TMAX')
        weather_tmax = weather_tmax.select(col('station'), col('date'), col('value').alias('tmax'))
        weather_tmin = weather_filtered_qflag.filter(weather_filtered_qflag.observation == 'TMIN')
        weather_tmin = weather_tmin.select(col('station'), col('date'), col('value').alias('tmin'))
        weather_max = weather_tmax.join(weather_tmin, (weather_tmax.station == weather_tmin.station) & (weather_tmax.date == weather_tmin.date)).drop(weather_tmin.station).drop(weather_tmax.date)
        weather_max.cache()
        weather_max = weather_max.withColumn('range', (col('tmax')-col('tmin')).cast("Int")/10)
        weather_max = weather_max.select(col('date'), col('station'), col('range'))
        weather_max.cache()
        aggregate_func = {'range' : 'max'}
        weather_range_max = weather_max.groupby('date').agg(aggregate_func)
        weather_range_max = weather_range_max.sort('date', ascending=True).select(col('date'), col('max(range)').alias('range')) 
        weather_range_max.cache()
        weather_final = weather_range_max.join(weather_max, (weather_range_max.date == weather_max.date) & (weather_range_max.range == weather_max.range )).drop(weather_range_max.date).drop(weather_range_max.range)
        weather_final = weather_final.sort(['date','station'], ascending=True)
        #weather_final.show(300)
        weather_final.write.csv(output, mode='overwrite') 
    
if __name__ == '__main__':
        inputs = sys.argv[1] 
        output = sys.argv[2]
        main(inputs, output)
