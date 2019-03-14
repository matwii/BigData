from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *

sc = SparkContext(appName="albums")
spark = SparkSession(sc).builder.getOrCreate()

albums_file = sc.textFile("./albums.csv")
artists_file = sc.textFile("./artists.csv")

albums = albums_file.map(lambda line: line.split(','))
albums = albums.map(lambda album: (int(album[0]), int(album[1]), album[3], int(album[4])))

artists = artists_file.map(lambda line: line.split(','))
artists = artists.map(lambda artist: (int(artist[0]), int(artist[4]), artist[5]))

album_fields = [
    StructField('id', IntegerType(), True),
    StructField('album_id', IntegerType(), True),
    StructField('genre', StringType(), True),
    StructField('year_of_pub', IntegerType(), True),
]

artist_fields = [
    StructField('id', IntegerType(), True),
    StructField('year_of_birth', IntegerType(), True),
    StructField('country', StringType(), True),
]

album_schema = StructType(album_fields)
album_df = spark.createDataFrame(albums, album_schema)

artist_schema = StructType(artist_fields)
artist_df = spark.createDataFrame(artists, artist_schema)

album_df.createOrReplaceTempView("albums")
artist_df.createOrReplaceTempView("artists")

#Number of distinct artists
dist_artists = spark.sql('SELECT COUNT (DISTINCT id) AS Number_of_artists from artists')
dist_artists.show()

#Number of distinct albums
dist_albums = spark.sql('SELECT COUNT (DISTINCT id) AS Number_of_albums from albums')
dist_albums.show()

#Number of distinct genres
dist_genres = spark.sql('SELECT COUNT (DISTINCT genre) AS Number_of_genres from albums').show()

#Number of distinct countries
dist_countries = spark.sql('SELECT COUNT (DISTINCT country) AS Number_of_countries from artists').show()

#Minimum year_of_pub
min_year_of_pub = spark.sql('SELECT MIN(year_of_pub) from albums').show()

#aximum year_of_pub
max_year_of_pub = spark.sql('SELECT MAX(year_of_pub) from albums').show()

#Minimum year_of_birth
min_year_of_birth = spark.sql('SELECT MIN(year_of_birth) from artists').show()

#Maximum year_of_birth
max_year_of_birth = spark.sql('SELECT MAX(year_of_birth) from artists').show()
