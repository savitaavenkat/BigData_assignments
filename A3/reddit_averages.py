from pyspark import SparkConf, SparkContext
import sys
import operator
import string
import json

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

def split_input(line):
        w = json.loads(line)
        yield(w["subreddit"], (w["score"], 1))

def get_key(kv):
        k, v = kv
        [k1, v1] = (k, v)
        return k1

def to_int(kv): # function to convert count to int
        (k, v) = kv
        [K2, V2] = (k, v)
        pair = list(V2)
        pair[0] = int(pair[0])
        V2 = tuple(pair)
        (k1, v1) = [K2, V2]
        k1v1 = (k1, v1)
        return k1v1

def add_pairs(v, v1):
        temp = ((v[0] + v1[0]), (v[1] + v1[1]))
        return temp

def divide_pairs(kv):
        k, v = kv
        [k2, v2] = (k, v)
        v2 = v2[0]/v2[1]
        (K1, V1) = [k2, v2]
        K1V1 = (K1, V1)
        return K1V1

def main(inputs, output):
        # main logic starts here
        text = sc.textFile(inputs)
        words = text.flatMap(split_input)
        words_count_int = words.map(to_int)
        added_pairs = words_count_int.reduceByKey(add_pairs)
        wordcount = added_pairs.map(divide_pairs)
        outdata = wordcount.sortBy(get_key).map(lambda x: json.dumps(x))
        outdata.saveAsTextFile(output)


if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit average')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
