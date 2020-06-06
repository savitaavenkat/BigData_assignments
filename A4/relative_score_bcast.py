from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3,5)
import json

def create_pairs(value):
        count = 1
        subreddit_name = value.get("subreddit")
        score = (count, value.get("score"))
        return (subreddit_name, score)

def create(data):
        return json.loads(data)

def add_pairs(p1,p2):
        return(p1[0]+p2[0],p1[1]+p2[1])

def get_avg(val):
        average = val[1][1]/val[1][0]
        if(average > 0):
                return(val[0], average)

def relative_score(broadcastaverage, relscore):
        return relscore['score']/broadcastaverage.value[relscore['subreddit']], relscore['author']

def main(inputs, output):
        text = sc.textFile(inputs)
        cached_json = text.map(lambda t : create(t)).cache()
        data_pairs = cached_json.map(lambda val : create_pairs(val))
        reducedoutput = data_pairs.reduceByKey(lambda p1,p2 : add_pairs(p1, p2))
        average = reducedoutput.map(get_avg)
        dictavg = dict(average.collect())
        broadcastavg = sc.broadcast(dictavg)
        finalmap = cached_json.map(lambda relscore : relative_score(broadcastavg, relscore)).sortByKey(ascending=False)
        finalmap.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit relative score')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
