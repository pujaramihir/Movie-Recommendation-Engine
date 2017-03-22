from pyspark.sql import SparkSession

def findSimilarity((movieId,genreString)):
    genre1 = genreString.split('|')
    sampleData = {}
    sampleData = getDisctionary()
    
    for g1 in genre1:
        sampleData[g1] = 1
        
    intersactionValue = 0;
    unionValue = 0;
    for (a1,b1) in zip(sampleData.values(),sampleMovieData.values()):
        if(int(a1)+int(b1) == 2):
            intersactionValue = intersactionValue + 1
            unionValue = unionValue + 1
        elif(int(a1)+int(b1) == 1):
            unionValue = unionValue + 1
    
    similarity = 0.00
    similarity = float(intersactionValue)/float(unionValue)
    return (sampleMovie[0].movieId, movieId, similarity)
    

def getDisctionary():
    similarityGenres = {}
    for genre in genresData:
        similarityGenres[genre.genres] = 0;        
    
    return similarityGenres


spark = SparkSession \
    .builder \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/") \
    .getOrCreate()

movies = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database","movielens").option("collection", "movies").load()
genres = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database","movielens").option("collection", "genres").load()

genresData = genres.collect()




sampleMovie = movies.filter(movies["movieId"] == 122904).collect()
sampleMovieData = getDisctionary()
if(len(sampleMovie) > 0):
    for i in sampleMovie[0].genres.split('|'):
        sampleMovieData[i] = 1


    

abcd = movies.filter(movies['genres'] != "(no genres listed)").rdd.map(lambda l: (l.movieId,l.genres)).map(findSimilarity)

#print sampleMovieData
print abcd.collect()
print "Bye" 


#print "Hello"
#movies.rdd.map(lambda l: (l.movieId,l.genres))
    


    
"""
genre = db.rdd.map(lambda l: l.genres).map(lambda l: l.split("|"))



data = genre.collect()

distinctGenres = []

for d in data:
    for d1 in d:
        distinctGenres.append(d1)
        
        
        
distinctGenres = list(set(distinctGenres))

print distinctGenres



data = sc.textFile("s3n://sundog-spark/ml-1m/ratings.dat")

# Map ratings to key / value pairs: user ID => movie ID, rating
ratings = data.map(lambda l: l.split("::")).map(lambda l: (int(l[0]), (int(l[1]), float(l[2]))))

# Emit every movie rated together by the same user.
# Self-join to find every combination.
ratingsPartitioned = ratings.partitionBy(100)
joinedRatings = ratingsPartitioned.join(ratingsPartitioned)

# At this point our RDD consists of userID => ((movieID, rating), (movieID, rating))

# Filter out duplicate pairs
uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)

# Now key by (movie1, movie2) pairs.
moviePairs = uniqueJoinedRatings.map(makePairs).partitionBy(100)

# We now have (movie1, movie2) => (rating1, rating2)
# Now collect all ratings for each movie pair and compute similarity
moviePairRatings = moviePairs.groupByKey()

# We now have (movie1, movie2) = > (rating1, rating2), (rating1, rating2) ...
# Can now compute similarities.
moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).persist()

# Save the results if desired
moviePairSimilarities.sortByKey()
moviePairSimilarities.saveAsTextFile("movie-sims")

# Extract similarities for the movie we care about that are "good".
if (len(sys.argv) > 1):

    scoreThreshold = 0.97
    coOccurenceThreshold = 1000

    movieID = int(sys.argv[1])

    # Filter for movies with this sim that are "good" as defined by
    # our quality thresholds above
    filteredResults = moviePairSimilarities.filter(lambda((pair,sim)): \
        (pair[0] == movieID or pair[1] == movieID) \
        and sim[0] > scoreThreshold and sim[1] > coOccurenceThreshold)

    # Sort by quality score.
    results = filteredResults.map(lambda((pair,sim)): (sim, pair)).sortByKey(ascending = False).take(10)

    print("Top 10 similar movies for " + nameDict[movieID])
    for result in results:
        (sim, pair) = result
        # Display the similarity result that isn't the movie we're looking at
        similarMovieID = pair[0]
        if (similarMovieID == movieID):
            similarMovieID = pair[1]
        print(nameDict[similarMovieID] + "\tscore: " + str(sim[0]) + "\tstrength: " + str(sim[1]))     
"""