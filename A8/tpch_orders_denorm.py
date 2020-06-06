import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types

cluster_seeds = ['199.60.17.188', '199.60.17.216']
spark = SparkSession.builder.appName('Spark Cassandra example') \
    .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

def output_line(kv):
    orderkey, price, names = kv
    namestr = ', '.join(sorted(list(names)))
    return ('Order #%d $%.2f: %s' % (orderkey, price, namestr))

def main(keyspace,output,orderkeys):

    orders_parts_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='orders_parts', keyspace=keyspace).load()
    orders_parts_df.registerTempTable('orders_parts_table')
    query = "SELECT orderkey,totalprice as price, part_names FROM orders_parts_table ORDER BY orderkey"
    ordered_df = spark.sql(query)

    orderkeys_list = []
    for iter in orderkeys:
        orderkeys_list.append(int(iter))

    filtered_df = ordered_df.filter(ordered_df.orderkey.isin(*orderkeys_list) == True)
    final_df = filtered_df.rdd.map(tuple).map(output_line)
    final_df.saveAsTextFile(output)

if __name__ == '__main__':
    keyspace = sys.argv[1]
    output = sys.argv[2]
    orderkeys = sys.argv[3:]
    main(keyspace,output,orderkeys)
