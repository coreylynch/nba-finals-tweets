from collections import Counter
from pymongo import Connection
import re

connection = Connection('localhost', 27017, tz_aware=True)

db = connection['nba_tweets']
locations = [i['user']['location'] for i in db.tweets.find() if 'user' in i and 'location' in i['user']]

cnt = Counter()
for location in locations:
    cnt[location]+=1

mypunct = ''.join(i for i in [i for i in string.punctuation if i != '@'])
punctre = re.compile('[%s]' % re.escape(mypunct))

with open('nba_graph.csv','wb') as f:
	for i in db.tweets.find():
		if 'text' in i and 'user' in i and 'RT @' in i['text']:
			text = i['text']
			rt_chunk = [j for j in text.split() if 'RT' in j][0]
			to_index = text.split().index(rt_chunk)+1
			to_node =  text.split()[to_index]
			if to_node[-1] == ':':
				to_node = to_node[:-1]
			from_node = punctre.sub('','@'+i['user']['name'])
			if from_node[-1] == ' ':
				from_node = from_node[:-1]
			if to_node != '' and from_node != '':
#				network.append((from_node.encode('utf8'),to_node.encode('utf8')))
				f.write('"'+from_node.encode('utf8')+'","'+to_node.encode('utf8')+'"')
				f.write('\n')
