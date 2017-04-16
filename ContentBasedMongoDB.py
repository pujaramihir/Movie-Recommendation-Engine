from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType
import sys
import numpy
from sympy.matrices.densetools import row

# $example off$

def computeUserProfile((userId,ratingList)):
    sum = 0.0
    totalRating = len(ratingList)
    multipliedVector = []
    for (movieId,(rating,itemprofile)) in ratingList:
        sum += float(rating)
        
    avgRating =  sum/totalRating
    for (movieId,(rating,itemprofile)) in ratingList:
        multipliedVector.append(rowMultiplecation(float(rating)/avgRating, itemprofile))
    
    return multipliedVector
    
    itemVectors = sumOfVectors(multipliedVector)
    userprofiles = rowDevision(totalRating, itemVectors)
    
    return userprofiles

def rowMultiplecation(value,rowdata):
    keys = rowdata.__fields__
    for key in keys:
        rowdata = rowdata.withColumn(key,rowdata[key] * value)    
    return rowdata

def sumOfVectors(vectorData):
    tempRow = {}
    if len(vectorData) > 0:
        keys = vectorData[0].keys()
        
        for key in keys:
            tempRow[key] = 0
            
        for vectorTemp in vectorData:
            keys = vectorTemp.keys()
            for key in keys:
                tempRow[key] += vectorTemp[key] 
    
    return tempRow

def rowDevision(value,rowdata):
    keys = rowdata.keys()
    for key in keys:
        rowdata[key] = rowdata[key] / value
    
    return rowdata


def getMovieName(id):
    tempData = movies.filter(movies["movieId"] == str(id)).collect()
    if len(tempData) > 0:
        return tempData[0].title.encode('ascii', 'ignore')

arguments = sys.argv

spark = SparkSession \
    .builder \
    .appName("Content_Based_Using_MongoDB")\
    .config("spark.mongodb.input.uri", "mongodb://"+arguments[1]+"/") \
    .config("spark.mongodb.output.uri", "mongodb://"+arguments[1]+"/") \
    .getOrCreate()

itemprofile = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database", "movielens").option("collection", "itemprofile").load().persist(StorageLevel.MEMORY_AND_DISK)
ratings = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database", "movielens").option("collection", "ratings1").load().persist(StorageLevel.MEMORY_AND_DISK)

items = itemprofile.rdd.collect()
itemprofiles = {}

for item in items:
    keyitem = item.movieId
    itemprofiles[int(keyitem)] = item.profile


ratingData = ratings.filter(ratings['userId'] == int(arguments[2])).rdd

ratingsData = ratingData.map(lambda l: (int(l.movieId), float(l.rating)))
itemprofilemap = itemprofile.rdd.map(lambda l : (int(l.movieId),l.profile))

userprofiledata =  ratingsData.join(itemprofilemap)

userprofiledata = userprofiledata.map(lambda l: (int(arguments[2]),l)).groupByKey().mapValues(list)

userprofile = userprofiledata.map(computeUserProfile)

print userprofile.collect()

#userProfile = ratingsData.map(computeUserProfile)
 
