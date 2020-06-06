# -*- coding: utf-8 -*-
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions
from pyspark.streaming import StreamingContext
import sys
import operator
import string
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+//

# add functions here
def consumer(topic):
        consumer = KafkaConsumer(topic, bootstrap_servers=['199.60.17.210', '199.60.17.193'])
        for msg in consumer:
                print(msg.value.decode('utf-8'))

def main(topic):
        messages = spark.readStream.format('kafka') \
                .option('kafka.bootstrap.servers', '199.60.17.210:9092,199.60.17.193:9092') \
                .option('subscribe', topic).load()
        values = messages.select(messages['value'].cast('string'))
        split_values = functions.split(values['value'], ' ')
        xy = values.withColumn("x",split_values.getItem(0)).withColumn("y",split_values.getItem(1)).drop('value')
        xy = xy.select(xy["x"].cast("float"),xy["y"].cast("float"))
        xy_values = xy.withColumn("x2",xy["x"]**2)
        xy_values = xy_values.withColumn("xy",xy["x"]*xy["y"])
        xy_values = xy_values.withColumn("n",functions.lit(1))
        xy_values = xy_values.groupby().sum()
        xy_values = xy_values.withColumnRenamed('sum(x)',"sumx").withColumnRenamed('sum(y)',"sumy").withColumnRenamed('sum(xy)',"sumxy").withColumnRenamed('sum(x2)',"sumx2").withColumnRenamed('sum(n)',"sumn")

        beta = xy_values.withColumn("beta",(xy_values["sumxy"]-(xy_values["sumx"]*xy_values["sumy"]/xy_values["sumn"]))/(xy_values["sumx2"]-(xy_values["sumx"]**2/xy_values["sumn"])))
        alpha = beta.withColumn("alpha",beta["sumy"]/beta["sumn"]-beta["beta"]*beta["sumx"]/beta["sumn"])
        result = alpha.select(alpha["beta"],alpha["alpha"])
        stream_df = result.writeStream.format('console').outputMode('update').start()
        stream_df.awaitTermination(600)


if __name__ == '__main__':
        spark = SparkSession \
                .builder \
                .appName("read_stream") \
                .getOrCreate()
        spark.sparkContext.setLogLevel('WARN')
        assert spark.version >= '2.3'  # make sure we have Spark 2.3+
        inputs = sys.argv[1]
        main(inputs)
