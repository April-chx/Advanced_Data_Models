import json
from pymongo import MongoClient

client = MongoClient()
con = client.online_music.user_artist_tag

# given a user id, find the most recent 10 artists the user has assigned tag to.
given_userID = 11
pipeline = [
    { "$match": { "userID": given_userID } },
    { "$group": { "_id":{ "userID": "$userID", "artistID": "$artistID", "artist_name": "$artist_name"}, "timestamp": { "$max": "$timestamp" } } },
    { "$sort": {"timestamp": -1 } },
    { "$limit":10 }
]
cursor = con.aggregate(pipeline)

for document in cursor:
    print json.dumps(document, encoding="UTF-8", ensure_ascii=False)