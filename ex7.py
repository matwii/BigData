from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from operator import add

conf = SparkConf().setMaster("local").setAppName("ex4")
sc = SparkContext(conf = conf)

albums = sc.textFile("./albums.csv")
artists = sc.textFile("./artists.csv")

#Transform our input data
def parseAlbums(line):
    line = line.split(',')
    id = line[0]
    artist_id = line[1]
    rolling_stone_critic = float(line[7])
    mtv_critic = float(line[8])
    music_maniac_critic = float(line[9])
    return (artist_id, id, rolling_stone_critic, mtv_critic, music_maniac_critic)

def parseArtists(line):
    line = line.split(',')
    id = int(line[0])
    country = line[5]
    return (id, country)


rdd_albums = albums.map(parseAlbums)
average_score_album = rdd_albums.map(lambda x: (int(x[0]), (int(x[1]), (x[2]+x[3]+x[4])/3)))
top_ten_albums = sc.parallelize(average_score_album.top(10, lambda x: x[1][1]))

rdd_artists = artists.map(parseArtists)

join = top_ten_albums.join(rdd_artists)
result = join.map(lambda x: (x[1][0][0], x[1][0][1], x[1][1]))

result.map(lambda x: '\t'.join([str(word) for word in x]))\
    .coalesce(1).saveAsTextFile('results/result_7.tsv')

print(result.collect())


