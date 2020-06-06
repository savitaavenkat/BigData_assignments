from pyspark import SparkConf, SparkContext
import sys
import random
import operator
import re, string
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def euler_function(sample_chunks):
        total_iterations = 0
        random.seed()
        for x in sample_chunks:
                sum = 0.0
                while sum < 1:
                        sum += random.uniform(0, 1) # to calculate sum of random numbers between 0,1
                        total_iterations = total_iterations + 1 # keeps count of the total iterations per sample
        return total_iterations

def add_partitions(v, v1): # function to add the iterations across samples together
        return v + v1

def main(inputs):
    # main logic starts here    
        sample_chunks = sc.range(inputs, numSlices=10).glom()
        samples_aggregate = sample_chunks.map(euler_function)
        output = samples_aggregate.reduce(add_partitions)
        print(output/inputs) # divide the total iterations across samples by the total number of samples to get 'e'

if __name__ == '__main__':
        conf = SparkConf().setAppName("euler's constant")
        sc = SparkContext(conf=conf)
        assert sc.version >= '2.3'  # make sure we have Spark 2.3+
        inputs = int(sys.argv[1])
        main(inputs)
