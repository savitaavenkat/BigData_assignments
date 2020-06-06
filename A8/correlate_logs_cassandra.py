from pyspark import SparkConf,SparkContext
from pyspark.sql import Row, SparkSession
import sys, datetime
import re,uuid
import  math

from pyspark.sql import SQLContext, functions, types
from pyspark.sql.functions import *

cluster_seeds = ['199.60.17.188', '199.60.17.216']
spark = SparkSession.builder.appName('Spark Cassandra example') \
    .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()

sc = spark.sparkContext
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def main(inputs,table):
        # main logic starts here
        assert sc.version >= '2.3' # make sure we have Spark 2.3+

        spark = SQLContext(sc)
        correlate_df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table=table, keyspace=inputs).load().select("host", "bytes")
        correlate_df.createOrReplaceTempView('nasa_df')
        correlate_df.show(300)
        sum_xy_df = spark.sql("SELECT count(host) AS x, sum(bytes) AS y FROM nasa_df GROUP BY host")
        sum_xy_df.createOrReplaceTempView('nasa_xy')
        sum_xy_df.show(100)
        sq_xy_df = spark.sql("SELECT x, y, 1 AS count, x*x AS xpow2, y*y AS ypow2, x*y AS xy FROM nasa_xy")
        sq_xy_df.createOrReplaceTempView('nasa_xy_sq')
        sq_xy_df.show(100)
        final = spark.sql("SELECT sum(x), sum(y), sum(count), sum(xpow2), sum(ypow2), sum(xy) FROM nasa_xy_sq")
        final.show()

        correlate = final.collect()
        correlate_denominator = (math.sqrt((correlate[0][2]*correlate[0][3]) - (math.pow(correlate[0][0], 2))))*(math.sqrt((correlate[0][2]*correlate[0][4]) - (math.pow(correlate[0][1], 2))))
        r = ((correlate[0][2]*correlate[0][5]) - (correlate[0][0]*correlate[0][1]))/correlate_denominator
        print("Correlation Coefficient 'r':", r)
        print("rpow2:", r*r)

if __name__ == '__main__':

    inputs = sys.argv[1]
    table = sys.argv[2]
    main(inputs,table)
