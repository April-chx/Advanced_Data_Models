import json
from pymongo import MongoClient

client = MongoClient()
con = client.online_music.user_friend_artists
music = client.online_music

# given a user id, find the top 5 artists listened by his friends but not him. We rank
# artists by the sum of friends' listening counts of the artist.
given_userID = 3

# given a user id, find the artists who are listening by friends and caculate the sum
# of friends' listening counts of the artist.
pipeline = [
    { "$match": { "userID": given_userID } },
    { "$group": { "_id": { "userID": "$userID", "friend_listen_artistID": "$friend_listen_artist.artistID"},
                  "friends_total_counts": { "$sum": "$friend_listen_artist.weight" } } },
    { "$out": "complex_query_3" }
]
friend_listened_artists = con.aggregate(pipeline)

# find the artist ids who are listening by users.
# remove the artists listened by user's friends but not user
user_listened_artists = con.find({ "friendID": given_userID }).distinct("friend_listen_artist.artistID")
for user_listened_artist in user_listened_artists:
    music.complex_query_3.remove({ "_id.friend_listen_artistID": user_listened_artist })

# sort the sum of friends' listening counts of the artist, find the top5 artists
top5_friends_artists = music.complex_query_3.find({ "_id.userID": given_userID }).sort([( "friends_total_counts", -1)]).limit(5)

for document in top5_friends_artists:
    print json.dumps(document, encoding="UTF-8", ensure_ascii=False)

music.complex_query_3.drop()