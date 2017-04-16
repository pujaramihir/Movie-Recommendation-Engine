from pyspark.sql import SparkSession
import csv
import json

spark = SparkSession \
    .builder \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/") \
    .getOrCreate()
    
    

data = sc.textFile("./Dataset/ml-latest/ratings.csv")
ratings = data.map(lambda l: l.split(",")).map(lambda l: (int(l[0]), int(l[1]), float(l[2])))
ratingsPartitioned = ratings.partitionBy(24)
ratings = spark.createDataFrame(ratingsPartitioned, ["userId","movieId","rating"])
ratings.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").option("database","movielens").option("collection", "ratings1").save()
ratings.show()    