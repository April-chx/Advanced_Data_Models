import json
from pymongo import MongoClient

client = MongoClient()
con = client.online_music.user_friend_artists

# given a user id, find all artists the user's friends listen
given_userID = 34
cursor = con.find({ "userID": given_userID }, { "_id": 0 })

for document in cursor:
    print json.dumps(document, encoding="UTF-8", ensure_ascii=False)