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

def main(input_keyspace, output_keyspace):
        # main logic starts here
        part_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='part', keyspace=input_keyspace).load()
        lineitem_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='lineitem', keyspace=input_keyspace).load()
        orders_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='orders', keyspace=input_keyspace).load()

        part_df.registerTempTable('part')
        lineitem_df.registerTempTable('lineitem')
        orders_df.registerTempTable('orders')

        query = """
                SELECT o.*, p.name as part_names  FROM orders o
                JOIN lineitem l ON o.orderkey=l.orderkey
                JOIN part p ON  l.partkey=p.partkey
                """

        joined_df = spark.sql(query)
        ordered_df = joined_df.orderBy(joined_df['orderkey']).dropDuplicates().cache()
        grouped_df = ordered_df.groupBy('orderkey', 'totalprice').agg(functions.collect_set('part_names'))
        selected_df = grouped_df.select(col("totalprice").alias("tprice"), col("orderkey").alias("ordkey"), col("collect_set(part_names)").alias("part_names"))

        ordered_df = ordered_df.drop(ordered_df['part_names'])

        ordered_df.registerTempTable('ordered_data_frame')
        selected_df.registerTempTable('group_data_frame')

        result_query = """
                        SELECT ord.*, grp.* FROM ordered_data_frame as ord
                        INNER JOIN group_data_frame as grp
                        ON (ord.orderkey == grp.ordkey) and (ord.totalprice == grp.tprice)
                        """
        
        final_df = spark.sql(result_query)
        final_df = final_df.drop('ordkey','tprice')
        final_df.show()
        final_df.write.format("org.apache.spark.sql.cassandra").options(table='orders_parts', keyspace=output_keyspace).save()

if __name__ == '__main__':
    
        input_keyspace  = sys.argv[1]
        output_keyspace = sys.argv[2]
        main(input_keyspace, output_keyspace)
