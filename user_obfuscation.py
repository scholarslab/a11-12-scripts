"""
A proof of concept script to demonstrate Twitter user ID obfuscation.

The problem is that we want to make it harder to match twitter IDs and
screennames to tweet texts, but still convey authorship information
across the corpus.

To perform this obfuscation, we take a hash (SHA3-224) of each UTF-8
encoded user ID and then use the first 60 bits to look up 4 words in
the EFF Diceware long word list.

The odds of a hash collision can be approximated by:
P≈1-e^(-(n^2/2H)) 
(per https://en.wikipedia.org/wiki/Birthday_problem#Approximations)

The charlottesville_2017_0814.json dataset contains 4735052 tweets and
1700875 unique users. 

Conservatively rounding the number of users up to 2 million across all
A11/12 datasets, this means there is a roughly 1 in 5780 chance of any
collision and roughly 1 in 576 billion chance that any given user ID
will have a doppelganger.

SHA3 is a crytographic hash, so knowing what how we do all of this
doesn't allow this process to be reversed. However, although SHA3 is
a relatively slow hash algorithm, the total number of twitter user ids
can be hashed in less than a minute.

We can either add a cryptographic salt of sufficient length and/or a
secret cryptographic pepper to to slow down hash precomputation. 
"""

# Import Python's json module
import json
import hashlib

# This should be secret
PEPPER = "gaodJx4W0neaLw5c-"

# Import the counter object from Python's collections module
# We could have just imported collections, but we'd have to refer to Counter as collections.Counter every time
from collections import Counter

# Load EFF Diceware list as a simple word list, but we're only going to use the first 2^12 (4096) values
wordlist = []
i = 0
with open("eff_large_wordlist.txt","r",encoding="utf-8") as infile:
    for line in infile.readlines():
        word_id,word = line.split()
        wordlist.append(word)

# open the file of 1000 tweets
filein = open("data/charlottesville_001000.jsonl","r")
# filein = open("data/charlottesville_20170814.json","r")

# extract a list of user ids
user_ids = set()
users = {}
for line in filein:
    j = json.loads(line)
    user_ids.add(j["user"]["id"])
    users[j["user"]["id"]] = j["user"]["screen_name"]

obufscated_users  = {}

for id in user_ids:
    hex = hashlib.sha3_224((PEPPER+str(id)).encode('utf-8')).hexdigest()
    hashes = []
    for i in range(4):
        hashes.append(hex[3*i:3*(i+1)])
    dicewords = []
    for hash in hashes:
        dicewords.append(wordlist[int(hash,base=16)])
    obufscated_users[id] = "-".join(dicewords)

# #save the result
with open("data/user_obfuscation.txt", "w") as outfile:
    for id in obufscated_users.keys():
        outfile.write(("@"+users[id]+" -> @"+obufscated_users[id]+"\n"))