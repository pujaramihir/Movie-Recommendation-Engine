from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType
import numpy
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
# $example off$

def getMovieName(id):
    tempData = movies.filter(movies["movieId"] == str(id)).collect()
    if len(tempData) > 0:
        return tempData[0].title.encode('ascii', 'ignore')

spark = SparkSession \
    .builder \
    .appName("Collaborative Filtering New")\
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/") \
    .getOrCreate()


movies = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database", "movielens").option("collection", "movies").load().persist()
ratings = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database", "movielens").option("collection", "ratings1").load().persist()
#ratingsRDD = movies.map(lambda p: Row(userId=int(p.userId), movieId=int(p.movieId),rating=float(p.rating)))
#ratings = spark.createDataFrame(ratingsRDD)
ratingschangedTypedf = ratings.withColumn("rating", ratings["rating"].cast(DoubleType())) 
ratingschangedTypedf = ratingschangedTypedf.withColumn("userId", ratingschangedTypedf["userId"].cast(IntegerType())) 
ratingschangedTypedf = ratingschangedTypedf.withColumn("movieId", ratingschangedTypedf["movieId"].cast(IntegerType()))
ratingschangedTypedf = ratingschangedTypedf.filter(ratingschangedTypedf['userId'] < 100)
(training, test) = ratingschangedTypedf.randomSplit([0.8, 0.2])
# Build the recommendation model using ALS on the training data
# Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics

als = ALS(maxIter=10, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating")
model = als.fit(training)

# Evaluate the model by computing the RMSE on the test data
predictions = model.transform(test)
print predictions.filter(str(predictions['prediction']) != 'nan').rdd.collect()

evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print rmse
# $example off$

spark.stop()
