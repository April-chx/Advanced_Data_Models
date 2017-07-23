import json
from pymongo import MongoClient

client = MongoClient()
con = client.online_music.user_artist_tag

# given an artist name, find the top 20 tags assigned to it. The tags are ranked by the
# number of times it has been assigned to this artist
given_artist_name = "Duran Duran"
pipeline = [
    { "$match": { "artist_name": given_artist_name } },
    { "$group": { "_id": { "artistID": "$artistID", "artist_name": "$artist_name", "tagID": "$tagID", "tagValue": "$tagValue" },
                   "tag_times": { "$sum": 1 } } },
    { "$sort": { "tag_times": -1 } },
    { "$limit": 20 }
]
cursor = con.aggregate(pipeline)

for document in cursor:
    print json.dumps(document, encoding="UTF-8", ensure_ascii=False)