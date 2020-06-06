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
        wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))
        for w in wordsep.split(line.lower()): # converts input to lowercase and then splits it according to the psttern defined
                yield (w, 1)

def get_key(kv):
        return kv[0]

def output_format(kv):
        k, v = kv
        return '%s %i' % (k, v)

def is_empty(kv): # function to filter out empty strings
        k, v = kv
        if len(k) > 0:
                return True
        else:
                return False

text = sc.textFile(inputs)
words = text.flatMap(words_once)
words_filtered = words.filter(is_empty)
wordcount = words_filtered.reduceByKey(operator.add)
          
outdata = wordcount.sortBy(get_key).map(output_format)
outdata.saveAsTextFile(output)
