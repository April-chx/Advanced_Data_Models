import json
from pymongo import MongoClient

client = MongoClient()
con = client.online_music.user_artist_info

# given an artist name, find the top 5 similar artists. Here similarity between a pair of
# artists is defined by the number of unique users that have listened both. The higher
# the number, the more similar the two artists are.
given_artist_name = "Duran Duran"
given_artist_id = con.find_one({ 'artist_name': given_artist_name })

# find the unique users who listen the artist.
user_ids = con.find({ "artist_name": given_artist_name }).distinct("userID")

# find all artists, except given artist, listened by unique users
similarity_list = {}
for u_id in user_ids:
    artistIDs = con.find({ "userID": u_id }).distinct("artistID")
    artistIDs.remove(given_artist_id['artistID'])

    # count the similarity between a pair of artists is defined by the number of unique users that have listened both
    for artistID in artistIDs:
        if artistID in similarity_list:
            similarity_list[artistID][0] += 1
        else:
            similarity_list[artistID] = [1]

# sort the similarity
similarity_list= sorted(similarity_list.iteritems(), key=lambda d:d[1], reverse = True)

# find the top5 similar artists and output artists' information
for similarity in similarity_list[0:5]:
    artist_info = con.find_one({ 'artistID': int(similarity[0]) })
    artist_similarity = { 'artistID': artist_info['artistID'], 'artist_name': artist_info['artist_name'], 'similarity': similarity[1], 'url': artist_info['url'], 'pictureURL': artist_info['pictureURL']}
    print json.dumps(artist_similarity, encoding="UTF-8", ensure_ascii=False)