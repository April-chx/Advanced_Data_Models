import json
from pymongo import MongoClient

client = MongoClient()
con = client.online_music.user_artist_tag

# given an artist name, find the most recent 10 tags that have been assigned to it.
given_artist_name = "Duran Duran"
cursor = con.find({ "artist_name": given_artist_name }, { "_id": 0 }).sort([( "timestamp", -1)]).limit(10)

for document in cursor:
    print json.dumps(document, encoding="UTF-8", ensure_ascii=False)