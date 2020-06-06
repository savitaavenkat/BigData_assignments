import sys

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('Wikipedia Popular DataFrame').getOrCreate()
assert sys.version_info >= (3, 5) # make sure we have Pythonspark = SparkSession.builder.appName('example code').getOrCreate() 3.5+
assert spark.version >= '2.3' # make sure we have Spark 2.3+

# add more functions as necessary
@functions.udf(returnType=types.StringType())
def path_to_hour(path):
        path = path.split('/')
        path = path[-1]
        path = path.lstrip('pagecounts-')
        path = path[0:11]
        return path

def main(inputs, output):
        # main logic starts here
        wikipedia_schema = types.StructType([
        types.StructField('language', types.StringType(), True),
        types.StructField('title', types.StringType(), True),
        types.StructField('views', types.LongType(), True),
        types.StructField('bytes', types.LongType(), True),
])
        wikipedia_popular = spark.read.csv(inputs, schema=wikipedia_schema, sep=" ").withColumn(inputs, path_to_hour(input_file_name()))
        wikipedia_popular.cache()
        wikipedia_filtered = wikipedia_popular.filter(wikipedia_popular.language == 'en')
        wikipedia_filtered_title = wikipedia_filtered.filter(wikipedia_filtered.title != 'Main_Page')
        wikipedia_filter = wikipedia_filtered_title.filter(wikipedia_filtered.title.startswith('Special:') == False)
        wikipedia_filter.cache()
        wikipedia_filter = wikipedia_filter.select(col(inputs).alias('hour'), col('title'), col('views'))
        max_func = {'views': 'max'}
        wikipedia_max = wikipedia_filter.groupby(wikipedia_filter['hour']).agg(max_func)
        wikipedia = wikipedia_max.sort(wikipedia_max['hour'], ascending=True)
        wikipedia = wikipedia.select(col('hour'), col('max(views)').alias('views'))
        wikipedia.cache()
        wikipedia_joined = wikipedia.join(wikipedia_filter, (wikipedia_filter.views == wikipedia.views) & (wikipedia_filter.hour == wikipedia.hour)).drop(wikipedia.views).drop(wikipedia.hour)
        wikipedia_joined.cache()
        wikipedia_joined_sorted  = wikipedia_joined.select(col('hour'), col('title'), col('views')).sort(['hour', 'title'], ascending=True) 
        #wikipedia_joined_sorted.show(200)
        wikipedia_joined_sorted.write.json(output, mode='overwrite')

if __name__ == '__main__':
        inputs = sys.argv[1]
        output = sys.argv[2] 
        main(inputs, output)
