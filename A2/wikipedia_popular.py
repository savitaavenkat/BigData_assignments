from pyspark import SparkConf, SparkContext
import sys
import operator
import re, string

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('word count')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

def words_once(line):
        w = line.split(' ')
        page_key = w[0]
        page_language = w[1]
        page_title = w[2]
        page_count = w[3]
        page_bytes = w[4]
        if page_language == "en":
                yield(page_key,(page_title, page_count))

def get_key(kv):
        return kv[0]

def tab_separated(kv):
        return "%s\t%s" % (kv[0], kv[1])

def to_int(kv): # function to convert count to int
        (k, v) = kv
        [K2, V2] = (k, v)
        pair = list(V2)
        pair[1] = int(pair[1])
        temp = pair[0] # swapping the place of count and page title to match the expected output
        pair[0] = pair[1]
        pair[1] = str(temp)
        V2 = tuple(pair)
        (k1, v1) = [K2, V2]
        k1v1 = (k1, v1)
        return k1v1

def find_max(v, v1):
        if v[0] < v1[0]:
                return v1
        elif v[0] > v1[0]:
                return v
        elif v[0] == v1[0]:
                store = (v[0], v[1] + ' ,' + v1[1]) # to store all pages that have the same max count
                return store

def filter_condition(kv):
        k, v = kv
        if not v[1].startswith("Special:"):
                if not v[1] == "Main_Page":
                        return True
        else:
                return False

text = sc.textFile(inputs)
words = text.flatMap(words_once)
words_count_int = words.map(to_int)
words_filtered = words_count_int.filter(filter_condition)
wordcount = words_filtered.reduceByKey(find_max)

outdata = wordcount.sortBy(get_key).map(tab_separated)
outdata.saveAsTextFile(output)

