from pyspark import SparkContext, SparkConf
conf = SparkConf()
sc = SparkContext(conf=conf)

import sys
import argparse

parser = argparse.ArgumentParser(description='TDT4305 phase 2 task')


#-user -k -file -output arguments into parser based on input
parser.add_argument("-user", help="user name")
parser.add_argument("-k", help="number of users", type=int)
parser.add_argument("-file", help="full path of the tweets file")
parser.add_argument("-output", help="full path of the output file")

args = parser.parse_args(sys.argv[1:])

#testing parameters
print(args.user, args.k, args.file, args.output)

tweets_file = sc.textFile(args.file)
tweets_rdd = tweets_file.map(lambda x: x.split('\t'))

# First maps through every word in user tweet
# Then takes a key-value pair as a new key based on username and word in tweet
# At last we reduce by key and sums how many times the user has used the same word and sorts it based on word count
words_per_user = tweets_rdd.flatMapValues(lambda x: x.split()).map(lambda x: ((x[0], x[1]), 1)).reduceByKey(lambda x, y: x + y).sortBy(lambda x: -x[1])

# Get word and frequency of input user
words_input_user = words_per_user.filter(lambda x: x[0][0] == args.user).map(lambda x: (x[0][1], x[1]))
#get word and frequency of all other users except input. We also take the username here
words_users = words_per_user.filter(lambda x: x[0][0] != args.user).map(lambda x: (x[0][1], (x[0][0], x[1])))

#make a join on the same words input user and the other users have used
words_join = words_users.join(words_input_user)

similar_users = words_join.map(lambda x: (x[1][0][0], min(x[1][0][1], x[1][1]))).reduceByKey(lambda x,y: x + y)
sorted_similar_users = similar_users.sortBy(lambda x: x[0])

top_k_similar_users = sc.parallelize(sorted_similar_users.take(args.k))

top_k_similar_users.map(lambda x: x[0] + '\t' + str(x[1])).coalesce(1, shuffle=True).saveAsTextFile(args.output)