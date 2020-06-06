from pyspark import SparkConf, SparkContext
import sys, operator
import json
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def graph_edges(node_val):
        l1 = node_val.split(':')
        if l1[1] == '':
                return None
        else:
                first_part = l1[0]

        l2 = (l1[1]).split()
        first_second = []
        for second_part in l2:
                first_second.append((first_part, second_part))
        return (first_second)

def minimum_distance(src_dest1,src_dest2):
    if src_dest1[1]<=src_dest2[1]:
        return src_dest1
    else:
        return src_dest2

# main logic
def main(inputs,output,source_node,destination_node):
        conf = SparkConf().setAppName('Correlation coefficient')
        sc = SparkContext(conf=conf)
        assert sc.version >= '2.3' # make sure we have Spark 2.3+

        text = sc.textFile(inputs + '/links-simple-sorted.txt')
        graph_edges_rdd = text.map(lambda node_val: graph_edges(node_val))
        graph_edges_rdd = graph_edges_rdd.filter(lambda x: x is not None).flatMap(lambda x: x)
        graph_edges_rdd = graph_edges_rdd.map(lambda x: tuple(map(int, x)))
        graph_edges_rdd.coalesce(1).saveAsTextFile(output)
        knownpaths = sc.parallelize([(source_node, ('-',0))])

        for i in range(6):
                join_on_kp = graph_edges_rdd.join(knownpaths.filter(lambda x : x[1][1]==i))
                nextpath = join_on_kp.map(lambda x : (x[1][0], (x[0], x[1][1][1]+1)))
                knownpaths = knownpaths.union(nextpath)
                knownpaths = knownpaths.reduceByKey(minimum_distance).cache()
                knownpaths.saveAsTextFile(output + '/iter-' + str(i))
                if knownpaths.lookup(destination_node)!=[]:
                        break
    
        finalpath = []
        flag = 0
        while destination_node!='-' :
                finalpath.append(destination_node)
                lookUpVal = knownpaths.lookup(destination_node)
                try:
                        destination_node = lookUpVal[0][0]
                except IndexError:
                        flag = 1
                        print('Destination Node:', destination_node,'not reachable!')
                        break

        # Code will print NULL in output path and throw an error stating the reason if the destination node is not reachable
        if flag != 1:
                finalpath = finalpath[::-1]
                print(finalpath)
                finalpath = sc.parallelize(finalpath)
                finalpath.saveAsTextFile(output + '/path')
        else:
                finalpath = 'NULL'
                print('Not reachable', finalpath)
                finalpath = sc.parallelize(finalpath)
                finalpath.saveAsTextFile(output + '/path')


if __name__ == '__main__':
        inputs = sys.argv[1]
        output = sys.argv[2]
        source_node = int(sys.argv[3])
        destination_node = int(sys.argv[4])
        main(inputs,output,source_node,destination_node)
