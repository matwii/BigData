from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from operator import add

conf = SparkConf().setMaster("local").setAppName("project1")
sc = SparkContext(conf = conf)

albums = sc.textFile("./albums.csv")
artists = sc.textFile("./artists.csv")

#Transform our input data
def parseLine(line):
    genre = line[3].split('-', 1)[-1]
    genre_length = len(genre.split())
    if (genre_length > 1):
        genre = genre.split(' ', 1)[1]
    return (genre)

#task_1
rdd = albums.map(lambda line: line.split(','))
genres = rdd.map(parseLine)
distinct_albums = genres.distinct()
result = distinct_albums.count()
print(distinct_albums.collect())
print (result)

#task_2
rdd_artists = artists.map(lambda line: line.split(','))
date_of_birth = rdd_artists.map(lambda line: line[4])
min_date = date_of_birth.min()
print(min_date)

#Task_3
artist_country = rdd_artists.map(lambda line: (line[5], 1))
artist_country_count = artist_country.reduceByKey(lambda x, y: x+y)
sort_by_count = artist_country_count.sortBy(lambda row: row[0]).sortBy(lambda row: -row[1])
print(sort_by_count.collect())

sort_by_count.map(lambda x: '\t'.join([str(word) for word in x])).coalesce(1).saveAsTextFile('results/result_2.tsv')



