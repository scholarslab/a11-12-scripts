"""
Display processing logic

This file contains the logic to transform Twitter
API dump jsonl files (typically saved as .json files)
into our archival display files, stripping out
deprecated, extraneous, and redundant data.

Input filenames go into the DATA_FILES list. Output
appears in the output directory.

Output separates tweet and user data to save space.

Performance seems good enough for our purposes
(6 seconds for 100k tweets).

"""

import json
from collections import defaultdict
from itertools import islice
from datetime import datetime
from collections import Counter


DATA_FILES = [
    # "charlottesville_0010.jsonl",
    # "charlottesville_0010000.jsonl"
    # "charlottesville_01000000.jsonl"
    # "charlottesville_20170814.json"
    "../../A11-12-Archive/Twitter/charlottesville_20170814.json"
    # "cvlf10k.json"
]

"""
Twitter API object documentation:
https://developer.x.com/en/docs/x-api/v1/data-dictionary/object-model/tweet
https://developer.x.com/en/docs/x-api/v1/data-dictionary/object-model/user

Twitter 1.1 docs cover changes not in place in 2017,
e.g. https://devcommunity.x.com/t/upcoming-changes-to-user-object-and-get-users-suggestions-endpoints/124732

Types of tweets display types:
Standalone
RT
QRT
Replies
RT of replies
QRT of replies
"""

TWEET_SCHEMA = {
    "text":["text"],
    "retweeted_status_id":["retweeted_status","id_str"],
    "id":["id_str"]
}

USER_SCHEMA = {
        "description":["description"],
        "location":["location"],
        "screen_name":["screen_name"],
        "name":["name"],
        "id":["id_str"],
}

'''
Follow the schema to extract a single display attribute
'''
def parse_attribute(source,twitter_element):
    for i in source:
        if i in twitter_element:
            twitter_element = twitter_element[i]
        else:
            return None
    return twitter_element

'''
follow the schema to extract the attributes for a single display tweet
'''
def extract_display_tweet(tweet):
    display_tweet = {}
    for target, source in TWEET_SCHEMA.items():
        att = parse_attribute(source,tweet)
        if att:
            display_tweet[target] = att
    return display_tweet

'''
Recursively return a twid:display_tweet dictionary of all upstream RT/QRT
'''
def extract_display_tweets(tweet):
    display_tweets = {}
    display_tweet = extract_display_tweet(tweet)
    # special hashtag processing
    display_hashtags = set()
    for hashtag in tweet["entities"]["hashtags"]:
        display_hashtags.add(hashtag["text"].lower())
    display_tweet["hashtags"] = list(display_hashtags)
    display_tweets[display_tweet["id"]] = display_tweet

    if "retweeted_status" in tweet:
        display_tweets.update(extract_display_tweets(tweet["retweeted_status"]))
    if "quoted_status" in tweet:
        display_tweets.update(extract_display_tweets(tweet["quoted_status"]))
    return display_tweets


'''
follow the schema to extract the attributes for a single display tweet
'''
def extract_display_user(tweet):
    display_user = {}
    for target, source in USER_SCHEMA.items():
        att = parse_attribute(source,tweet["user"])
        if att:
            display_user[target] = att
    return display_user


'''
Recursively return a twid:display_user dictionary of all upstream RT/QRT
'''
def extract_display_users(tweet):
    display_users = {}
    display_user = extract_display_user(tweet)
    display_users[display_user["id"]] = display_user
    if "retweeted_status" in tweet:
        display_users.update(extract_display_users(tweet["retweeted_status"]))
    if "quoted_status" in tweet:
        display_users.update(extract_display_users(tweet["quoted_status"]))
    return display_users

"""
Potentially useful data?
"source"
"possibly_sensitive" #user-selected
"lang" #machine-detected
user/"listed_count" #number of lists user is on

Notes:

1. Entities need  as part of this processing step
    to strip out url shortening, obfuscate users, etc.
2. retweeted_status has the original tweet, which contains some
    of the display values (favorites count, etc) used on the
    Twitter front end. We might want to use some of those
    values, depending on how we decide to handle RTs.
"""
display_tweets = {}
display_users = {}
retweets = defaultdict(str)
counter = 0

retweets_count = Counter()

for filename in DATA_FILES:
    with open("./data/"+filename,"r",-1,"UTF-8") as infile:
        for line in infile:
            tweet = json.loads(line)
            # extract all display tweets from tweet 
            extracted_tweets = extract_display_tweets(tweet)
            # filter out tweets already extracted
            extracted_tweets = {k:v for k,v in extracted_tweets.items() if k not in display_tweets}
            # update main display tweets dçictionary with extracted tweets
            display_tweets.update(extracted_tweets)

            # remove retweets and append info to parent
            for extracted_tweet in extracted_tweets.values():
                if "retweeted_status_id" in extracted_tweet:
                   del display_tweets[extracted_tweet["id"]]
            
            # extract all users
            extracted_users = extract_display_users(tweet)
            users = {k:v for k,v in extracted_users.items() if k not in display_users}
            display_users.update(extracted_users)

    tw_user_texts = []
    for tw in display_tweets.values():
        tw_user_texts.append(tw["text"].replace("\n"," ")+"\n")
    u_user_texts = []
    for u in display_users.values():
        for k in USER_SCHEMA.keys():
            if k in u and k != "id":
                u_user_texts.append(u[k].replace("\n"," "))
        u_user_texts.append("\n")
    
    with open("./output/tw_usertext.json", "w", encoding="UTF-8") as outfile:
        for text in tw_user_texts:
            outfile.write(text+" ")
    
    with open("./output/u_usertext.json", "w", encoding="UTF-8") as outfile:
        for text in u_user_texts:
            outfile.write(text+" ")