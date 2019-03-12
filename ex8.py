from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from operator import add

conf = SparkConf().setMaster("local").setAppName("ex4")
sc = SparkContext(conf = conf)

albums = sc.textFile("./albums.csv")
artists = sc.textFile("./artists.csv")

rdd_albums = albums.map(lambda x: x.split(','))
top_rated_albums = rdd_albums.filter(lambda x: float(x[8])==5.0).map(lambda x: int(x[1]))

print(top_rated_albums.collect())

