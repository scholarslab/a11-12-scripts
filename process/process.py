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
    # "charlottesville_0010.jsonl",
    # "charlottesville_0010000.jsonl"
    # "charlottesville_01000000.jsonl",
    "charlottesville_20170814.json"
]

BOX_SIZE = 1000000

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
    "is_quote_status":["is_quote_status"],
    "in_reply_to_status_id":["in_reply_to_status_id_str"],
    "id":["id_str"],
    "favorite_count":["favorite_count"],
    "in_reply_to_screen_name":["in_reply_to_screen_name"],
    "in_reply_to_user_id":["in_reply_to_user_id_str"],
    "retweet_count":["retweet_count"],
    "created_at":["created_at"],
    "user_id":["user","id_str"],
    "user_screen_name":["user","screen_name"] # duplicated for lookup convenience
}

USER_SCHEMA = {
        "id":["id_str"],
        "verified":["verified"],
        "followers_count":["followers_count"],
        "statuses_count":["statuses_count"],
        "description":["description"],
        "friends_count":["friends_count"],
        "location":["location"],
        "utc_offset":["utc_offset"],
        "time_zone":["time_zone"],
        "screen_name":["screen_name"],
        "lang":["lang"], #user interface setting, deprecated in 1.1 but seemingly still available here
        "name":["name"],
        "url":["url"],
        "created_at":["created_at"]
}

def parse_schema(source,element):
    e = element
    for i in source:
        e = e[i]
    return str(e)

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
display_tweets = {}
display_users = {}
display_tweet_ids = []
display_tweet_id_dict = {}
for filename in DATA_FILES:
    with open("./data/"+filename,"r",-1,"UTF-8") as infile:
        display_tweets_box = {}
        file_count = 0
        for line in infile:
            tweet = json.loads(line)
            display_tweet = {}
            for target, source in TWEET_SCHEMA.items():
                display_tweet[target] = parse_schema(source,tweet)
            users = [tweet["user"]]
            if "retweeted_status" in tweet:
                users.append(tweet["retweeted_status"]["user"])
                display_tweet["retweeted_status"] = {}
                for target, source in TWEET_SCHEMA.items():
                    display_tweet["retweeted_status"][target] = parse_schema(source,tweet["retweeted_status"])
            else:
                display_tweet["retweeted_status"] = None
            display_tweets_box[tweet["id"]] = display_tweet
            display_tweet_ids.append(tweet["id"])
            # user processing
            for user in users:
                display_user = {}
                for target,source in USER_SCHEMA.items():
                    display_user[target] = parse_schema(source,user)
                display_users[display_user["id"]] = display_user

            if len(display_tweets_box) >= BOX_SIZE:
                print(filename,": Writing file",file_count)
                tweets_fn = "disp_tw_"+"".join(filename.split(".")[:-1])+"-"+str(file_count).zfill(3)+".json"
                with open("./output/"+tweets_fn, "w", encoding="UTF-8") as outfile:
                    json.dump(display_tweets_box,outfile)
                display_tweets.update(display_tweets_box)
                display_tweets_box = {}
                
                with open("./output/disp_twids_"+filename, "w", encoding="UTF-8") as outfile:
                    display_tweet_id_dict.update({twid: tweets_fn for twid in display_tweet_ids})
                    json.dump(display_tweet_id_dict,outfile)

                # users_fn = "disp_users_"+"".join(filename.split(".")[:-1])+"-"+str(file_count).zfill(3)+".json"
                # with open("./output/"+users_fn, "w", encoding="UTF-8") as outfile:
                #     json.dump(display_users,outfile)
                file_count+=1
                # if file_count>1:
                #     break
                # exit()
    
    print(filename,": Writing file",file_count)

    tweets_fn = "disp_tw_"+"".join(filename.split(".")[:-1])+"-"+str(file_count).zfill(3)+".json"
    with open("./output/"+tweets_fn, "w", encoding="UTF-8") as outfile:
        json.dump(display_tweets_box,outfile)
    
    tweets_fn = "disp_tw_"+"".join(filename.split(".")[:-1])+"-full.json"
    with open("./output/"+tweets_fn, "w", encoding="UTF-8") as outfile:
        json.dump(display_tweets,outfile)
                
    with open("./output/disp_twids_"+filename, "w", encoding="UTF-8") as outfile:
        display_tweet_id_dict.update({twid: tweets_fn for twid in display_tweet_ids})
        json.dump(display_tweet_id_dict,outfile)

    users_fn = "disp_users_"+"".join(filename.split(".")[:-1])+".json"
    with open("./output/"+users_fn, "w", encoding="UTF-8") as outfile:
        json.dump(display_users,outfile)
    
