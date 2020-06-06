import sys
import datetime
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('weather tomorrow').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')

from pyspark.sql.types import DateType
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import DecisionTreeClassifier,RandomForestClassifier
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
])

def main(model):
    tmax_table = [('Campus',datetime.datetime.strptime('13-11-2018','%d-%m-%Y'),49.2771,-122.9146,330.0,12.0),\
                   ('Campus',datetime.datetime.strptime('12-11-2018','%d-%m-%Y'),49.2771,-122.9146,330.0,12.0)]

    df = spark.createDataFrame(tmax_table, schema=tmax_schema)
    pipeline = PipelineModel.load(model)
    Campus_tmax = pipeline.transform(df)
    prediction =  Campus_tmax.select(Campus_tmax['prediction'])
    prediction = prediction.collect()[0].asDict()['prediction']
    print('Predicted tmax tomorrow:', prediction)

if __name__ == '__main__':
    model = sys.argv[1]
    main(model)
