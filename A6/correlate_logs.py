import sys
import re
import math

from pyspark.sql import SparkSession, SQLContext, functions, types, Row
from pyspark.sql.functions import *
from pyspark import SparkConf, SparkContext

assert sys.version_info >= (3, 5) # make sure we have Pythonspark = SparkSession.builder.appName('example code').getOrCreate() 3.5+ 

# add more functions as necessary
def input_split(line): # function to split the nasa_logs into groups as needed
        line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
        line_group = line_re.split(line)
        count = 1
        line_group_split = re.match(line_re, line)
        if line_group_split:
                row = re.search(line_re, line)
                host_name = row.group(1)
                host_bytes = float(row.group(4))
                return host_name, host_bytes
        return None

def is_empty(kv): # function to filter out empty strings, if any
        k, v = kv
        if len(k) > 0:
                return True
        else:
                return False

def to_df_format(logs):
        (k, v) = logs
        row = Row(hostname = k, bytes = v)
        return (row)

def main(inputs):
        # main logic starts here
        conf = SparkConf().setAppName('Correlation coefficient')
        sc = SparkContext(conf=conf)
        assert sc.version >= '2.3' # make sure we have Spark 2.3+

        schema = types.StructType([
        types.StructField('hostname', types.StringType(), False),
        types.StructField('bytes', types.FloatType(), False),
        ])

        text = sc.textFile(inputs)
        nasa_logs_split = text.map(input_split).filter(lambda x : x is not None)
        nasa_logs_filtered = nasa_logs_split.filter(is_empty)
        nasa_logs_formatted = nasa_logs_filtered.map(to_df_format)

        spark = SQLContext(sc)
        correlate_df = spark.createDataFrame(nasa_logs_formatted, schema)
        correlate_df.createOrReplaceTempView('nasa_df')
        sum_xy_df = spark.sql("SELECT count(hostname) AS x, sum(bytes) AS y FROM nasa_df GROUP BY hostname")
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
        main(inputs)
