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
from datetime import datetime, timezone, timedelta
from collections import Counter


DATA_FILES = [
    # "charlottesville_0010.jsonl",
    # "charlottesville_0010000.jsonl"
    "charlottesville_01000000.jsonl",
    # "charlottesville_20170814.json"
]

BOX_SIZE = 2000
# EDT is UTC-4, EST is UTC-5
TZ_OFFSET = -4
OUTPUT_FILENAME = "cville814"

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

# maximum number of followers to qualify as small, medium, and large sized accounts
USER_SIZE_BOUNDS = [1000,100000,1000000]

TWEET_SCHEMA = {
    "text":["text"],
    "retweeted_status_id":["retweeted_status","id_str"],
    "quoted_status_id":["quoted_status_id"],
    "in_reply_to_status_id":["in_reply_to_status_id_str"],
    "id":["id_str"],
    "favorite_count":["favorite_count"],
    "in_reply_to_screen_name":["in_reply_to_screen_name"],
    "in_reply_to_user_id":["in_reply_to_user_id_str"],
    "retweet_count":["retweet_count"],
    "created_at":["created_at"],
    "user_id":["user","id_str"],
    "user_screen_name":["user","screen_name"], # duplicated for lookup convenience
    "verified":["user","verified"], # duplicated for lookup convenience
}

USER_SCHEMA = {
        "id":["id_str"],
        "verified":["verified"],
        "followers_count":["followers_count"],
        # "statuses_count":["statuses_count"],
        "description":["description"],
        "friends_count":["friends_count"],
        "location":["location"],
        # "utc_offset":["utc_offset"],
        # "time_zone":["time_zone"],
        "screen_name":["screen_name"],
        # "lang":["lang"], #user interface setting, deprecated in 1.1 but seemingly still available here
        "name":["name"],
        # "url":["url"],
        # "created_at":["created_at"]
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
        display_user[target] = parse_attribute(source,tweet["user"])
    # construct search fields (0 is smallest)
    if not display_user["followers_count"]:
        display_user["followers_count"] = 0
    display_user["size"] = len(USER_SIZE_BOUNDS)
    for size in reversed(range(len(USER_SIZE_BOUNDS))):
        if int(display_user["followers_count"]) < USER_SIZE_BOUNDS[size]:
            display_user["size"] = size
    
    # created_at fields
    date_format = "%a %b %d %H:%M:%S %z %Y"
    created_at = datetime.strptime(display_user["created_at"], date_format)
    display_user["created_at"] = int(created_at.timestamp())

    local_date_str = created_at.astimezone(timezone(timedelta(hours=TZ_OFFSET))).strftime('%Y-%m-%d')
    display_user["created_year"] = created_at.strftime('%Y')
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

def chunk_dictionary(input_dict, chunk_size):
    dict_iterator = iter(input_dict.items())
    for i in range(0, len(input_dict), chunk_size):
        yield dict(islice(dict_iterator, chunk_size))

# test = json.load(open("./data/test.json","r",encoding="utf-8"))
# print(json.dumps(extract_display_tweets(test),indent=3))
# print(json.dumps(extract_display_users(test),indent=3))
# exit()

"""
Potentially useful data?
"source"
"possibly_sensitive" #user-selected
"lang" #machine-detected
user/"listed_count" #number of lists user is on

Notes:

1. Entities need as part of this processing step
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
            
            # convert datetime string to unix epoch
            for twid,extracted_tweet in extracted_tweets.items():
                dtstr = extracted_tweet["created_at"]
                # print(twid)
                # print(json.dumps(extracted_tweet,indent=2))
                date_format = "%a %b %d %H:%M:%S %z %Y"
                dt = datetime.strptime(dtstr, date_format)
                local_date_str = dt.astimezone(timezone(timedelta(hours=TZ_OFFSET))).strftime('%Y-%m-%d')
                # local_time_str = dt.astimezone(timezone(timedelta(hours=TZ_OFFSET))).strftime('%H:%M:%S')
                timestamp = dt.timestamp()
                extracted_tweet["created_at"] = int(timestamp)
                extracted_tweet["local_date"] = local_date_str
                # extracted_tweet["local_time"] = local_time_str
            
            # update main display tweets dictionary with extracted tweets
            display_tweets.update(extracted_tweets)

            # remove retweets and append info to parent
            for extracted_tweet in extracted_tweets.values():
                if "retweeted_status_id" in extracted_tweet:
                    parent = display_tweets[extracted_tweet["retweeted_status_id"]]  
                    if "retweets" not in parent:
                        parent["retweets"] = []
                    parent["retweets"].append((extracted_tweet["user_id"],extracted_tweet["user_screen_name"],extracted_tweet["created_at"]))
                    # parent["retweets"].append((extracted_tweet["user_id"],extracted_tweet["user_screen_name"]))
                    del display_tweets[extracted_tweet["id"]]
            
            # extract all users
            extracted_users = extract_display_users(tweet)
            # since we're going backward in time, don't overwrite older data with newer
            users = {k:v for k,v in extracted_users.items() if k not in display_users}
            display_users.update(extracted_users)

            counter+=1
            if counter % BOX_SIZE == 0:
                print("Processing tweet #"+str(counter))
            # if counter > BOX_SIZE*1:
            #     break
    
    # for id,tweet in display_tweets.items():
    #     if "retweets" in tweet:
    #         retweets_count[tweet["id"]] = len(tweet["retweets"])
    
    # for twid,retweet_count in retweets_count.most_common(20):
    #     print(twid, retweet_count, "retweets")
    
    file_count = 0
    display_twids = {}
    for chunk in chunk_dictionary(display_tweets, BOX_SIZE):
        print("Writing tweet file",str(file_count)+"/"+str(int(len(display_tweets)/BOX_SIZE)))
        print("  (",len(chunk),"tweets )")
        tweets_fn = "disp_tw_"+OUTPUT_FILENAME+"-"+str(file_count).zfill(3)+".json"
        with open("./output/"+tweets_fn, "w", encoding="UTF-8") as outfile:
            json.dump(chunk,outfile)
        for twid,tweet in chunk.items():
            display_twids[twid] = tweets_fn
        file_count+=1

    with open("./output/disp_twids_"+OUTPUT_FILENAME+".json", "w", encoding="UTF-8") as outfile:
        json.dump(display_twids,outfile)

    
    # create sort lists
    with open("./output/sort_chrono_"+OUTPUT_FILENAME+".json", "w", encoding="UTF-8") as outfile:
        json.dump(sorted(display_tweets.keys()),outfile)
    with open("./output/sort_favs_"+OUTPUT_FILENAME+".json", "w", encoding="UTF-8") as outfile:    
        json.dump([display_tweet["id"] for display_tweet in sorted(display_tweets.values(), key=lambda t: int(t.get("favorite_count",0)), reverse=True)],outfile)
    with open("./output/sort_retweets_"+OUTPUT_FILENAME+".json", "w", encoding="UTF-8") as outfile:
        json.dump([display_tweet["id"] for display_tweet in sorted(display_tweets.values(), key=lambda t: int(t.get("retweet_count",0)), reverse=True)],outfile)
    with open("./output/sort_followers_"+OUTPUT_FILENAME+".json", "w", encoding="UTF-8") as outfile:
        json.dump([display_tweet["id"] for display_tweet in sorted(display_tweets.values(), key=lambda t: int(display_users[t["user_id"]]["followers_count"]), reverse=True)],outfile)
    
    file_count = 0
    display_userids = {}
    for chunk in chunk_dictionary(display_users, BOX_SIZE):
        print("Writing user file",str(file_count)+"/"+str(int(len(display_tweets)/BOX_SIZE)))
        print("  (",len(chunk),"users )")
        users_fn = "disp_u_"+OUTPUT_FILENAME+"-"+str(file_count).zfill(3)+".json"
        with open("./output/"+users_fn, "w", encoding="UTF-8") as outfile:
            json.dump(chunk,outfile)
        for userid,user in chunk.items():
            display_userids[userid] = users_fn
        file_count+=1
            
    with open("./output/disp_userids_"+OUTPUT_FILENAME+".json", "w", encoding="UTF-8") as outfile:
        json.dump(display_userids,outfile)