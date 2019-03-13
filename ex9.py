from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from operator import add

conf = SparkConf().setMaster("local").setAppName("ex4")
sc = SparkContext(conf = conf)

albums = sc.textFile("./albums.csv")
artists = sc.textFile("./artists.csv")

rdd_artists = artists.map(lambda x: x.split(','))
rdd_albums = albums.map(lambda x: x.split(','))

filtered_artists=(rdd_artists.filter(lambda x: x[5]=="Norway").map(lambda x: (int(x[0]), x[2])))

filtered_albums = rdd_albums.map(lambda x: (int(x[1]), float(x[8])))
join = filtered_artists.join(filtered_albums)

nor_albums_by_score = join.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: ((x[0][0],x[0][1] + y[0][1]), x[1]+y[1]))
average_by_score = nor_albums_by_score.mapValues(lambda x: (x[0][0], (x[0][1] / x[1])))
result = average_by_score.map(lambda x: (x[1][0], x[1][1], 'Norway')).sortBy(lambda x: x[0]).sortBy(lambda x: -x[1])

result.map(lambda x: '\t'.join([str(word) for word in x]))\
    .coalesce(1).saveAsTextFile('results/result_9.tsv')

print(result.collect())



