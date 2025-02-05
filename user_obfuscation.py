"""
A proof of concept script to demonstrate Twitter user ID
pseudonymization.

The problem is that we want to make it harder to match twitter IDs and
screennames to collected tweet texts, but still convey authorship
information across the corpus. Replacing the user id and screen name
with a consistent and unique pseudonym is a reasonable solution.

Ideally, a pseudonym should be legible at a glance and not too long in
length.  In this pseudonymization example, we take a hash (SHA3-224) of
each UTF-8 encoded user ID and then use the first 36 bits to look up 3
words in the EFF Diceware long word list.

One immediate problem with this approach is that we need a low
probability of a hash collision, which would cause two users to share
the same pseudonym. 

The odds of a hash collision can be approximated by:
P≈1-e^(-(n^2/2H)) 
(per https://en.wikipedia.org/wiki/Birthday_problem#Approximations)

The charlottesville_2017_0814.json dataset contains 4735052 tweets and
1700875 unique authors. Conservatively rounding the number of authors up
to 2 million across all A11/12 datasets, using only three words
virtually guarantees a collision somewhere in the dataset even if the
probability that any given user has a doppelganger is relatively low.

Increasing the pseudonym to 6 words (72 bits) decreases the collision
chance to 1 in 23 million, but a 6-word pseudonym is pretty unwieldy.
We can also increase the size of the word list to buy more entropy, but
the nice thing about the EFF diceware list is that it's been sanitized
of problematic words (insults, profanity, slurs, and words evocative of
trauma and abuse).

Instead, we can reduce the colision chance by appending an alphanumeric
postfix to serve as a sort of checksum. 

Using a base 33 alphabet (3-Z, since 0,1, and 2 all have similar-looking 
letters), we can form a 5-character postfix using the next 25 bits of
the hash. With 61 bits of entropy, the overall chance of a hash
collision in our user set is reduced to under 1 in 11000.

Each pseudonym would be something in this general format:
@exceeding-cut-fascism-E5FQP

SHA3 is a crytographic hash, so knowing how we do all of this doesn't
in itself allow this process to be reversed. However, although SHA3 is
a relatively slow hash algorithm, the total number of twitter user ids
is fairly small and can be hashed in less than a minute.

To mitigate this kind of distributed attack, we can either add a
cryptographic salt of sufficient length and/or a secret cryptographic
pepper to to slow down hash precomputation. 

Of course none of this is really secure - the original tweets are
largely extant on the live Twitter service, so it would be trivial to
find a different tweet from the same user and then map it to the user
through Twitter's (rather bad) search. Obfuscating usernames will
still allow deleted accounts to remain pseudonymous and at least slows
down the identification process for others.

"""

# Import Python's json module
import json
import hashlib

# This should be secret
PEPPER = "gaodJx4W0neaLw5c"


def bin_to_base33(binary):
    """
    Convert a binary string to a readable base 33 (3-Z) string.
    (0, 1, 2 have similar-looking latin characters, so we omit them)
    """
    decimal_value = int(binary,2)  # Convert bin to decimal
    base26_chars = '3456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    if decimal_value == 0:
        return "0"

    base26 = ""
    while decimal_value:
        decimal_value, remainder = divmod(decimal_value, 26)
        base26 = base26_chars[remainder] + base26

    return base26

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
    for i in range(3):
        hashes.append(hex[3*i:3*(i+1)])
    dicewords = []
    for hash in hashes:
        dicewords.append(wordlist[int(hash,base=16)])
    alpha = bin_to_base33(bin(int(hex[9:],base=16))[:25])
    postfix = "0"*(5-len(alpha))+alpha
    obufscated_users[id] = "-".join(dicewords)+"-"+postfix

# #save the result
with open("data/user_obfuscation.txt", "w") as outfile:
    for id in obufscated_users.keys():
        outfile.write(("@"+users[id]+" -> @"+obufscated_users[id]+"\n"))