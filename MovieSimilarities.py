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


    

similarities = movies.filter(movies['genres'] != "(no genres listed)").rdd.map(lambda l: (l.movieId,l.genres)).map(findSimilarity)


print similarities.collect()


    
"""
genre = db.rdd.map(lambda l: l.genres).map(lambda l: l.split("|"))



data = genre.collect()

distinctGenres = []

for d in data:
    for d1 in d:
        distinctGenres.append(d1)
        
        
        
distinctGenres = list(set(distinctGenres))

print distinctGenres
"""