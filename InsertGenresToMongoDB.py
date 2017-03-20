from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/") \
    .getOrCreate()

db = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database","movielens").option("collection", "movies").load()

genre = db.filter(db['genres'] != "(no genres listed)").rdd.map(lambda l: l.genres).map(lambda l: l.split("|"))


data = genre.collect()

distinctGenres = []

for d in data:
    for d1 in d:
        distinctGenres.append(str(d1))
                
distinctGenres = list(set(distinctGenres))
temp = []
for d in distinctGenres:
    temp.append([d])

genres = spark.createDataFrame(temp, ["genres"])
genres.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").option("database","movielens").option("collection", "genres").save()
genres.show()    
