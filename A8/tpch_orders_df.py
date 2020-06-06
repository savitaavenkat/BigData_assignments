from pyspark import SparkConf,SparkContext
from pyspark.sql import Row, SparkSession
import sys, datetime
import re,uuid
import  math

from pyspark.sql import SQLContext, functions, types
from pyspark.sql.functions import *

cluster_seeds = ['199.60.17.188', '199.60.17.216']
spark = SparkSession.builder.appName('Spark Cassandra example') \
    .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).config('spark.dynamicAllocation.maxExecutors', 16).getOrCreate()

sc = spark.sparkContext
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


def output_line(kv):
    orderkey, price, names = kv
    namestr = ', '.join(sorted(list(names)))
    return 'Order #%d $%.2f: %s' % (orderkey, price, namestr)


def main(keyspace,output,orderkeys):
    # main logic starts here
    part_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='part', keyspace=keyspace).load()
    lineitem_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='lineitem', keyspace=keyspace).load()
    orders_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='orders', keyspace=keyspace).load()

    part_df.registerTempTable('part')
    lineitem_df.registerTempTable('lineitem')
    orders_df.registerTempTable('orders')

    query = """
        SELECT o.orderkey, o.totalprice, p.name FROM orders o
        JOIN lineitem l ON (o.orderkey = l.orderkey)
        JOIN part p ON (l.partkey = p.partkey)
        """

    joined_df = spark.sql(query)

    orderkeys_list = []
    for iter in orderkeys:
        orderkeys_list.append(int(iter))

    filtered_df = joined_df.filter(joined_df.orderkey.isin(*orderkeys_list) == True)
    ordered_df = filtered_df.orderBy(filtered_df['orderkey']).groupBy('orderkey', 'totalprice')
    collected_name_df = ordered_df.agg(functions.collect_set('name')) #example of collect_set to be used after joining tables together and required daa has been extracted//
    collected_name_df.show() #explain()
    collected_name_df.explain()
    final_df = collected_name_df.rdd.map(tuple).map(output_line)
    final_df.saveAsTextFile(output)


if __name__ == '__main__':

    keyspace = sys.argv[1]
    output = sys.argv[2]
    orderkeys = sys.argv[3:]
    main(keyspace,output,orderkeys)
