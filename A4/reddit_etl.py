from pyspark import SparkConf, SparkContext
import sys
import operator
import string
import json

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

def split_input(line):
        w = json.loads(line)
        yield(w["subreddit"], (w["score"],w["author"]))

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

def positive_sub(kv):
        if(kv[1][0] > 0):
                return kv

def negative_sub(kv):
        if(kv[1][0] <= 0):
                return kv

def subreddit_filter(kv):
        k, v = kv
        [k2, v2] = (k, v)
        key = k2
        temp = "e"
        if temp in key:
                return kv
        else:
                pass


def main(inputs, output):
        # main logic starts here
        text_input = sc.textFile(inputs)
        subreddit_rows = text_input.flatMap(split_input)
        subreddit_score_int = subreddit_rows.filter(subreddit_filter).map(to_int)
        subreddit_score_int.cache()
        positive_subreddit = subreddit_score_int.filter(positive_sub)
        pos = positive_subreddit.sortBy(get_key).map(lambda x: json.dumps(x))
        negative_subreddit = subreddit_score_int.filter(negative_sub)
        neg = negative_subreddit.sortBy(get_key).map(lambda x: json.dumps(x))
        neg.saveAsTextFile(output + '/negative')
        pos.saveAsTextFile(output + '/positive')


if __name__ == '__main__':
	conf = SparkConf().setAppName('reddit etl')
	sc = SparkContext(conf=conf)
	assert sc.version >= '2.3'  # make sure we have Spark 2.3+
	inputs = sys.argv[1]
	output = sys.argv[2]
	main(inputs, output)
