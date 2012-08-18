import sys
from pymongo import Connection
from pymongo.errors import ConnectionFailure
import tweetstream
import string


def normalize(s):
	ret = s
	for p in string.punctuation:
		ret = ret.replace(p, '')
	return ret


""" Connect to MongoDB """ 
try:
    c = Connection(host="localhost", port=27017) 
except ConnectionFailure, e:
    sys.stderr.write("Could not connect to MongoDB: %s" % e)
    sys.exit(1)

db=c['nba_tweets']


with open('nba_queries.txt') as f:
	queries = [i.strip() for i in f.readlines()]
	
with tweetstream.SampleStream('coreylynch','caligula9') as stream:
	for tweet in stream:
		if 'text' in tweet.keys() and len(tweet['text'])>0:
			if True in [i.lower() in queries for i in tweet['text'].split()]:
				print tweet['text']
				db.tweets.insert(tweet)
			