import json
from pymongo import MongoClient

client = MongoClient()
con = client.online_music.user_artist_info

# find the top 5 artists ranked by the number of users listening to it
pipeline = [
    { "$group": { "_id": {"artistID": "$artistID", "artist_name": "$artist_name", "url": "$url", "pictureURL": "$pictureURL"},
                   "num_of_users":{ "$sum": 1 } } },
    { "$sort": { "num_of_users": -1 } },
    { "$limit": 5 }
]
cursor = con.aggregate( pipeline )
for document in cursor:
    print json.dumps(document, encoding="UTF-8", ensure_ascii=False)