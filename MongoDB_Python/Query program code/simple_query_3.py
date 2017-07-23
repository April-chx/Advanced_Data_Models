import json
from pymongo import MongoClient

client = MongoClient()
con = client.online_music.user_artist_info

# given an artist name, find the top 10 users based on their respective listening counts
# of this artist. Display both the user id and the listening count
given_artist_name = "Duran Duran"
cursor = con.find({ "artist_name": given_artist_name },{ "_id": 0, "url": 0, "pictureURL": 0 }).sort([("weight", -1)]).limit(10)

for document in cursor:
    print json.dumps(document, encoding="UTF-8", ensure_ascii=False)