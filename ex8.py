from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from operator import add

conf = SparkConf().setMaster("local").setAppName("ex4")
sc = SparkContext(conf = conf)

albums = sc.textFile("./albums.csv")
artists = sc.textFile("./artists.csv")

rdd_albums = albums.map(lambda x: x.split(','))
rdd_artists = artists.map(lambda x: x.split(','))

filtered_albums = (rdd_albums.filter(lambda x: float(x[8])==5.0).map(lambda x: x[1]).collect())
filtered_artists = (rdd_artists.filter(lambda x: x[0] in filtered_albums).map(lambda x: x[2]).sortBy(lambda x: x).distinct().collect())

filtered_artists = list(filter(None, filtered_artists))

sc.parallelize(filtered_artists).map(lambda x: ''.join([str(word) for word in x]))\
    .saveAsTextFile('results/result_8.tsv')

print(filtered_artists)


