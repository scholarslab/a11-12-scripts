from lunr import lunr

import os
import json

# DATA_FILES = [
#     "charlottesville_0010.json",
#     "charlottesville_0100000.json"
# ]

DATA_FILES = []
for filename in os.listdir("./output"):
    if filename.startswith("disp_tw_") and filename.endswith(".json"):
        path = os.path.join("./output/", filename)
        DATA_FILES.append(path)

INDEX_TWEET_SCHEMA = [
    "id",
    "text",
    "user_screen_name",
    "hashtags"
]

'''
Builds index out of 
'''
index_tweets = []
for filename in DATA_FILES:
    with open(filename,"r",-1,"UTF-8") as infile:
        tweets = json.load(infile)
        for tweet in tweets.values():
            index_tweet = {}
            for target in INDEX_TWEET_SCHEMA:
                index_tweet[target] = tweet[target]
            index_tweets.append(index_tweet)

idx = lunr(ref=INDEX_TWEET_SCHEMA[0],fields=tuple(INDEX_TWEET_SCHEMA[1:]),documents=index_tweets)
serialized_idx = idx.serialize()
with open('./output/index.json', 'w') as outfile:
    json.dump(serialized_idx, outfile)