from pyspark.sql import SparkSession
import sys

def findSimilarity((movieId,genreString)):
    genre1 = genreString.split('|')
    sampleData = {}
    sampleData = getDisctionary()
    
    for g1 in genre1:
        sampleData[g1] = 1
           
    return (movieId,sampleData)
    

def getDisctionary():
    similarityGenres = {}
    for genre in genresData:
        similarityGenres[genre.genres] = 0;        
    
    return similarityGenres


arguments = sys.argv

spark = SparkSession \
    .builder \
    .appName("Content_Based_Using_MongoDB")\
    .config("spark.mongodb.input.uri", "mongodb://"+arguments[1]+"/") \
    .config("spark.mongodb.output.uri", "mongodb://"+arguments[1]+"/") \
    .getOrCreate()


movies = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database","movielens").option("collection", "movies").load()
genres = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database","movielens").option("collection", "genres").load()

genresData = genres.collect()

similarities = movies.filter(movies['genres'] != "(no genres listed)").rdd.map(lambda l: (l.movieId,l.genres)).map(findSimilarity)

itemProfile = spark.createDataFrame(similarities, ["movieId","profile"])
itemProfile.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").option("database","movielens").option("collection", "itemprofile").save()
itemProfile.show()  