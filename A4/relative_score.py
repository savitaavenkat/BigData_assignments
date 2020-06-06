from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3,5)


def create_pair(value):
        count = 1
        subreddit_name = value.get("subreddit")
        score = (count, value.get("score"))
        return (subreddit_name, score)

def create_RDD(data):
        return json.loads(data)

def add_pairs(pair1,pair2):
        return(pair1[0]+pair2[0],pair1[1]+pair2[1])

def get_avg(val):
        average = val[1][1]/val[1][0]
        if(average > 0):
                return(val[0], average)

def relative_score(value):
        return (value[1][0]['score']/value[1][1], value[1][0]['author'])

def main(inputs, output):
        text = sc.textFile(inputs)
        cached_json = text.map(lambda x : create_RDD(x)).cache()
        data_pairs = cached_json.map(lambda val : create_pair(val))
        reducedoutput = data_pairs.reduceByKey(lambda p1,p2 : add_pairs(p1, p2))
        average = reducedoutput.map(get_avg)
        cached_map = cached_json.map(lambda c: (c['subreddit'], c))
        joined_map = cached_map.join(average)
        final_map = joined_map.map(relative_score).sortByKey(ascending=False)
        final_map.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit relative score')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
