from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from operator import add

conf = SparkConf().setMaster("local").setAppName("ex4")
sc = SparkContext(conf = conf)

albums = sc.textFile("./albums.csv")
artists = sc.textFile("./artists.csv")

#Transform our input data
def parseLine(line):
    line = line.split(',')
    id = line[0]
    rolling_stone_critic = float(line[7])
    mtv_critic = float(line[8])
    music_maniac_critic = float(line[9])
    return (id, rolling_stone_critic, mtv_critic, music_maniac_critic)


rdd = albums.map(parseLine)
average_score_album = rdd.map(lambda x: (int(x[0]), (x[1]+x[2]+x[3])/3))
top_ten_albums = average_score_album.top(10, lambda x: x[1])
sc.parallelize(top_ten_albums).map(lambda x: '\t'.join([str(word) for word in x]))\
    .coalesce(1).saveAsTextFile('results/result_6.tsv')
print(top_ten_albums)
