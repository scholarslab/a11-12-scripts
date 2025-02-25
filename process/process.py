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


DATA_FILES = [
    "charlottesville_0010.json",
    "charlottesville_0100000.json"
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

TWEET_SCHEMA = [
    "text",
    "is_quote_status",
    "in_reply_to_status_id",
    "id",
    "favorite_count",
    "coordinates",
    "in_reply_to_screen_name",
    "in_reply_to_user_id",
    "retweet_count",
    "created_at"
]

USER_SCHEMA = [
        "verified",
        "followers_count",
        "statuses_count",
        "description",
        "friends_count",
        "location",
        "screen_name",
        "lang", #user interface setting, deprecated in 1.1 but seemingly still available here
        "name",
        "url",
        "created_at"
]

"""
Potentially useful data?
"source"
"possibly_sensitive" #user-selected
"lang" #machine-detected
user/"listed_count" #number of lists user is on

Notes:

1. Entities need to be extracted as part of this processing step
    to strip out url shortening, obfuscate users, etc.
2. retweeted_status has the original tweet, which contains some
    of the display values (favorites count, etc) used on the
    Twitter front end. We might want to use some of those
    values, depending on how we decide to handle RTs.
"""

for filename in DATA_FILES:
    with open("./data/"+filename,"r",-1,"UTF-8") as infile:
        display_tweets = []
        display_users = {}
        for line in infile:
            tweet = json.loads(line)
            display_tweet = {}
            display_user = {}
            for element in TWEET_SCHEMA:
                display_tweet[element] = tweet[element]
            if "retweeted_status" in tweet:                
                display_tweet["retweeted_status"] = {}
                for element in TWEET_SCHEMA:
                    display_tweet["retweeted_status"][element] = tweet["retweeted_status"][element]
            else:
                display_tweet["retweeted_status"] = None
            display_tweets.append(display_tweet)
            # Do entities/RT processing here...
            for element in USER_SCHEMA:
                display_user[element] = tweet["user"][element]
            user_id = tweet["user"]["id"]
            display_users[user_id] = display_user

    with open("./output/display_tweets_"+filename, "w", encoding="UTF-8") as outfile:
        json.dump(display_tweets,outfile)

    with open("./output/display_users_"+filename, "w", encoding="UTF-8") as outfile:
        json.dump(display_users,outfile)
