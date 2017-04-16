from pyspark.sql import SparkSession
from pyspark.mllib.recommendation import ALS, Rating
from pyspark.storagelevel import StorageLevel 


def getMovieName(id):
    tempData = movies.filter(movies["movieId"] == str(id)).collect()
    if len(tempData) > 0:
        return tempData[0].title.encode('ascii', 'ignore')

spark = SparkSession \
    .builder \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/") \
    .getOrCreate()


movies = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database","movielens").option("collection", "movies").load()
ratings = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database","movielens").option("collection", "ratings").load()

ratingsData = ratings.rdd.map(lambda l: Rating(int(l.userId), int(l.movieId), float(l.rating))).persist(StorageLevel.MEMORY_AND_DISK)


#print ratingsData.collect()

rank = 10
# Lowered numIterations to ensure it works on lower-end systems
numIterations = 10
model = ALS.train(ratingsData, rank, numIterations)

userID = 260

print("\nRatings for user ID " + str(userID) + ":")
userRatings = ratingsData.filter(lambda l: l[0] == userID)
for rating in userRatings.collect():
    print getMovieName(int(rating[1])) + ": " + str(rating[2])

print("\nTop 10 recommendations:")
recommendations = model.recommendProducts(userID, 10)
for recommendation in recommendations:
    print getMovieName(int(recommendation[1])) + \
        " score " + str(recommendation[2])
        
#movieMap = movies.rdd.map(lambda l: )