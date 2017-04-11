from pyspark.sql import SparkSession
import csv
import json

spark = SparkSession \
    .builder \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/") \
    .getOrCreate()
    
    
with open('./Dataset/ml-latest/splitratingfiles/fileaa') as f:
    reader = csv.reader(f)
    rows = tuple(reader)
    

"""
list = []

for row in rows:
    a = [] 
    a.append(int(row[0]))
    a.append(int(row[1]))
    a.append(float(row[2]))
    a.append(int(row[3]))
    list.append(a)
"""

ratings = spark.createDataFrame(rows, ["userId","movieId","rating","timestamp"])
ratings.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").option("database","movielens").option("collection", "ratings1").save()
ratings.show()    