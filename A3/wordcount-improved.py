from pyspark import SparkConf, SparkContext
import sys
import operator
import re, string
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

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

def main(inputs, output):
    # main logic starts here
        text = sc.textFile(inputs).repartition(8)
        words = text.flatMap(words_once)
        words_filtered = words.filter(is_empty).reduceByKey(operator.add)
        outdata = words_filtered.sortBy(get_key).map(output_format)
        outdata.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('example code')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output) 
