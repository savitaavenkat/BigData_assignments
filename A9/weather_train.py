import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('weather_train').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')

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

def main(inputs,model_file):

    data = spark.read.csv(inputs, schema=tmax_schema)
    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()
    query = """
            SELECT  today.latitude, today.longitude, today.elevation, dayofyear(today.date) as day_of_year,
            today.tmax, yesterday.tmax AS yesterday_tmax FROM __THIS__ as today
            INNER JOIN __THIS__ as yesterday
            ON date_sub(today.date, 1) = yesterday.date
            AND today.station = yesterday.station
           """
    querywithoutyest = """SELECT  latitude, longitude, elevation, dayofyear(date) as day_of_year, tmax from __THIS__"""
    sqlTrans = SQLTransformer(statement=query)
    # sqlTrans = SQLTransformer(statement=querywithoutyest)
    feature_cols = ["latitude", "longitude", "elevation", "day_of_year", "yesterday_tmax"]
    # feature_cols = ["latitude", "longitude", "elevation", "day_of_year"]

    feature_assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    classifier = GBTRegressor(featuresCol='features', labelCol='tmax', maxIter=100)
    pipeline = Pipeline(stages=[sqlTrans, feature_assembler, classifier])
    train_model = pipeline.fit(train)
    predictions = train_model.transform(validation)
    predictions.show()
    evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax',metricName='r2')
    score = evaluator.evaluate(predictions)
    print('Validation score for Temperature prediction model r2: %g' % (score))
    rmseevaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax',metricName='rmse')
    rmsescore = rmseevaluator.evaluate(predictions)
    print('Validation score for Temperature prediction model rmse: %g' % (rmsescore))
    train_model.write().overwrite().save(model_file)

if __name__ == '__main__':
    inputs = sys.argv[1]
    model_file = sys.argv[2]
    main(inputs,model_file)
