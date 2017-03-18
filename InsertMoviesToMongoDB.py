from pyspark.sql import SparkSession
import csv
import json

spark = SparkSession \
    .builder \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/") \
    .getOrCreate()
    
    
with open('./Dataset/ml-latest/movies.csv') as f:
    reader = csv.reader(f)
    rows = tuple(reader)
    


movies = spark.createDataFrame(rows, ["movieId","title","genres"])
movies.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").option("database","movielens").option("collection", "movies1").save()
movies.show()    