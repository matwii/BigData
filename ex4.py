from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from operator import add

conf = SparkConf().setMaster("local").setAppName("ex4")
sc = SparkContext(conf = conf)

albums = sc.textFile("./albums.csv")
artists = sc.textFile("./artists.csv")

rdd = albums.map(lambda line: line.split(','))
albums_artist = rdd.map(lambda line: (int(line[1]), 1))
albums_artist_count = albums_artist.reduceByKey(lambda x, y: x+y)
result = albums_artist_count.sortBy(lambda row: -row[0]).sortBy(lambda row: -row[1])

print(result.collect())

result.map(lambda x: '\t'.join([str(word) for word in x])).coalesce(1).saveAsTextFile('results/result_3.tsv')

