from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType

# $example off$

def getMovieName(id):
    tempData = movies.filter(movies["movieId"] == str(id)).collect()
    if len(tempData) > 0:
        return tempData[0].title.encode('ascii', 'ignore')

spark = SparkSession \
    .builder \
    .appName("Content_Based_Using_MongoDB")\
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/") \
    .getOrCreate()


movies = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database", "movielens").option("collection", "movies").load().persist()
ratings = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database", "movielens").option("collection", "ratings1").load().persist()

ratingData = ratings.filter(ratings['userId'] == 9).rdd.map(lambda l: (int(l.userId), (int(l.movieId), float(l.rating)))).groupByKey().mapValues(list).persist(StorageLevel.MEMORY_AND_DISK)
print ratingData.collect()