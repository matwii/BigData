from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from operator import add

conf = SparkConf().setMaster("local").setAppName("project1")
sc = SparkContext(conf = conf)

albums = sc.textFile("./albums.csv")
artists = sc.textFile("./artists.csv")

genres = albums.map(lambda line: line.split(',')[3])
distinct_albums = genres.distinct()
result = distinct_albums.count()
print(distinct_albums.collect())
print (result)