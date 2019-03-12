from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from operator import add

conf = SparkConf().setMaster("local").setAppName("ex4")
sc = SparkContext(conf = conf)

albums = sc.textFile("./albums.csv")
artists = sc.textFile("./artists.csv")