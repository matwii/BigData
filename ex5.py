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
    genre = line[3]
    num_sales = int(line[6])
    return (genre, num_sales)

#task_6
genres = albums.map(parseLine)
result = genres.reduceByKey(lambda x, y: x+y).sortBy(lambda row: row[0])\
    .sortBy(lambda row: -row[1])

result.map(lambda x: '\t'.join([str(word) for word in x])).coalesce(1).saveAsTextFile('results/result_5.tsv')
