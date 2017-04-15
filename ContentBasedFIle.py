from pyspark import SparkConf,SparkContext

conffiguration = SparkConf().setMaster("local[*]").setAppName("LogAnalysis")
spark = SparkContext(conf = conffiguration)
spark.setLogLevel('ERROR')

movies = spark.textFile("movies.csv")
ratings = spark.textFile("ratings.csv")

ratingData = ratings.map(lambda l: l.split(",")).map(lambda l: (int(l[0]), (int(l[1]), float(l[2])))).groupByKey()

