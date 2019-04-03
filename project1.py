from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from operator import add

conf = SparkConf().setMaster("local").setAppName("project1")
sc = SparkContext(conf = conf)

albums = sc.textFile("./albums.csv")
artists = sc.textFile("./artists.csv")

def task1(albums):
    genres = albums.map(lambda line: line.split(',')[3])
    distinct_albums = genres.distinct()
    result = distinct_albums.count()
    print(distinct_albums.collect())
    print (result)

def task2(artists):
    date_of_birth = artists.map(lambda line: line.split(',')[4])
    min_date = date_of_birth.min()
    print(min_date)

def task3(artists):
    artist_country = artists.map(lambda line: (line.split(',')[5], 1))
    artist_country_count = artist_country.reduceByKey(lambda x, y: x + y)
    sort_by_count = artist_country_count.sortBy(lambda row: row[0]).sortBy(lambda row: -row[1])
    print(sort_by_count.collect())
    sort_by_count.map(lambda x: '\t'.join([str(word) for word in x])).coalesce(1) \
        .saveAsTextFile('results/result_3.tsv')

def task4(albums):
    albums_artist = albums.map(lambda line: (int(line.split(',')[1]), 1))
    albums_artist_count = albums_artist.reduceByKey(lambda x, y: x + y)
    result = albums_artist_count.sortBy(lambda row: -row[0]).sortBy(lambda row: -row[1])

    result.map(lambda x: '\t'.join([str(word) for word in x])).coalesce(1)\
        .saveAsTextFile('results/result_4.tsv')

def task5(albums):
    genres = albums.map(lambda line: (line.split(',')[3], int(line.split(',')[6])))
    result = genres.reduceByKey(lambda x, y: x + y).sortBy(lambda row: row[0]) \
        .sortBy(lambda row: -row[1])

    print(result.collect())

    result.map(lambda x: '\t'.join([str(word) for word in x]))\
        .coalesce(1).saveAsTextFile('results/result_5.tsv')

def task6(albums):
    # Transform our input data
    def parseLine(line):
        line = line.split(',')
        id = line[0]
        rolling_stone_critic = float(line[7])
        mtv_critic = float(line[8])
        music_maniac_critic = float(line[9])
        return (id, rolling_stone_critic, mtv_critic, music_maniac_critic)

    rdd = albums.map(parseLine)
    average_score_album = rdd.map(lambda x: (int(x[0]), (x[1] + x[2] + x[3]) / 3))
    top_ten_albums = average_score_album.top(10, lambda x: x[1])

    print(top_ten_albums)

    sc.parallelize(top_ten_albums).map(lambda x: '\t'.join([str(word) for word in x])) \
        .coalesce(1).saveAsTextFile('results/result_6.tsv')

def task7(albums, artists):
    # Transform our input data
    def parseAlbums(line):
        line = line.split(',')
        id = line[0]
        artist_id = line[1]
        rolling_stone_critic = float(line[7])
        mtv_critic = float(line[8])
        music_maniac_critic = float(line[9])
        return (artist_id, id, rolling_stone_critic, mtv_critic, music_maniac_critic)

    rdd_albums = albums.map(parseAlbums)
    average_score_album = rdd_albums.map(lambda x: (int(x[0]), (int(x[1]), (x[2] + x[3] + x[4]) / 3)))
    top_ten_albums = sc.parallelize(average_score_album.top(10, lambda x: x[1][1]))

    rdd_artists = artists.map(lambda line: (int(line.split(',')[0]), line.split(',')[5]))

    join = top_ten_albums.join(rdd_artists)
    result = join.map(lambda x: (x[1][0][0], x[1][0][1], x[1][1]))

    print(result.collect())

    result.map(lambda x: '\t'.join([str(word) for word in x])) \
        .coalesce(1).saveAsTextFile('results/result_7.tsv')

def task8(albums, artists):
    rdd_albums = albums.map(lambda x: x.split(','))
    rdd_artists = artists.map(lambda x: x.split(','))

    filtered_albums = (rdd_albums.filter(lambda x: float(x[8]) == 5.0).map(lambda x: x[1]).collect())
    filtered_artists = (rdd_artists.filter(lambda x: x[0] in filtered_albums).map(lambda x: x[2]).sortBy(
        lambda x: x).distinct().collect())

    filtered_artists = list(filter(None, filtered_artists))

    print(filtered_artists)

    sc.parallelize(filtered_artists).map(lambda x: ''.join([str(word) for word in x])) \
        .saveAsTextFile('results/result_8.tsv')

def task9(albums, artists):
    rdd_artists = artists.map(lambda x: x.split(','))
    rdd_albums = albums.map(lambda x: x.split(','))

    filtered_artists = (rdd_artists.filter(lambda x: x[5] == "Norway").map(lambda x: (int(x[0]), x[2])))

    filtered_albums = rdd_albums.map(lambda x: (int(x[1]), float(x[8])))
    join = filtered_artists.join(filtered_albums)

    nor_albums_by_score = join.mapValues(lambda x: (x, 1)).reduceByKey(
        lambda x, y: ((x[0][0], x[0][1] + y[0][1]), x[1] + y[1]))
    average_by_score = nor_albums_by_score.mapValues(lambda x: (x[0][0], (x[0][1] / x[1])))
    result = average_by_score.map(lambda x: (x[1][0], x[1][1], 'Norway')).sortBy(lambda x: x[0]).sortBy(lambda x: -x[1])

    print(result.collect())
    result.map(lambda x: '\t'.join([str(word) for word in x])) \
        .coalesce(1).saveAsTextFile('results/result_9.tsv')


task1(albums)
task2(artists)
task3(artists)
task4(albums)
task5(albums)
task6(albums)
task7(albums, artists)
task8(albums, artists)
task9(albums, artists)




