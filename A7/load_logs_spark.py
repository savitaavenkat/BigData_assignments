from pyspark import SparkConf,SparkContext
from pyspark.sql import Row, SparkSession
import sys, datetime
import re,uuid

cluster_seeds = ['199.60.17.188', '199.60.17.216']
spark = SparkSession.builder.appName('Spark Cassandra example') \
    .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
sc = spark.sparkContext
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def splitline(line):
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] "[A-Z]+ (\S+) HTTP/\d\.\d" \d+ (\d+)$')
    match = re.search(line_re, line)
    if match:
        str_ = re.search(line_re, line)
        host = str_.group(1)
        date = datetime.datetime.strptime(str_.group(2), '%d/%b/%Y:%H:%M:%S')
        path = str_.group(3)
        bytes = float(str_.group(4))
        id = str(uuid.uuid1())
        return (host, date, path, bytes, id)
    return None

def convert_to_row(tup_):
    (host,date,path,bytes,id) = tup_
    row = Row(host = host, datetime=date, path=path, bytes = bytes, id=id)
    return (row)

def main(inputs,keyspace,output):
    # main logic starts here
    host_bytes = sc.textFile(inputs).repartition(70)
    host_bytes = host_bytes.map(lambda line: splitline(line)).filter(lambda x: x is not None)
    rows = host_bytes.map(convert_to_row)
    data_frame = spark.createDataFrame(rows)
    data_frame.write.format("org.apache.spark.sql.cassandra") \
    .options(table=output, keyspace=keyspace, parallelism = 8).save()

if __name__ == '__main__':

    inputs = sys.argv[1]
    keyspace = sys.argv[2]
    output = sys.argv[3]
    main(inputs,keyspace,output)
