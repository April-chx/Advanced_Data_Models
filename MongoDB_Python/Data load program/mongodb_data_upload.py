from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client.online_music

with open('F:/Pycharm Projects/file_translater/comp5338/data_set/artists.dat') as f:
    data = f.read().split('\n')
    for line in data[1:]:
        if line.strip():
            sep = line.split('\t')
            artists_info = { '_id': int(sep[0]), 'name': sep[1], 'url': sep[2], 'pictureURL': sep[3] }
            db.artists.insert(artists_info)
print("File artists.dat imported")

artist = {}
friend_artists_list = {}
with open('F:/Pycharm Projects/file_translater/comp5338/data_set/user_artists.dat') as f:
    data = f.read().split('\n')
    for line in data[1:]:
        if line.strip():
            userID, artistID, weight = line.split('\t')
            friend_artists_list.setdefault(userID, {'friend_listen_artists':[]})
            find_artist = db.artists.find_one({'_id': int(artistID)})
            artist = {'artistID': int(artistID), 'name': find_artist['name'], 'url': find_artist['url'], 'pictureURL': find_artist['pictureURL'], 'weight': int(weight)}
            user_artist = {'userID': int(userID), 'artistID': int(artistID), 'artist_name': find_artist['name'], 'url': find_artist['url'], 'pictureURL': find_artist['pictureURL'], 'weight': int(weight)}
            friend_artists_list[userID]['friend_listen_artists'].append(artist)
            db.user_artist_info.insert(user_artist)
print("File user_artist_info imported")

user_friends_info = {}
with open('F:/Pycharm Projects/file_translater/comp5338/data_set/user_friends.dat') as f:
    data = f.read().split('\n')
    for line in data[1:]:
        if line.strip():
            userID, friendID = line.split('\t')
            friend_listen_artists = friend_artists_list[friendID]['friend_listen_artists']
            for artist_line in friend_listen_artists[0:]:
                user_friends_info = { 'userID': int(userID), 'friendID': int(friendID), 'friend_listen_artist': artist_line}
                db.user_friend_artists.insert(user_friends_info)
print("File user_friend_artists imported")

with open('F:/Pycharm Projects/file_translater/comp5338/data_set/tags.dat') as f:
    data = f.read().split('\n')
    for line in data[1:]:
        if line.strip():
            tagID, tagValue = line.split('\t')
            tags_info = { '_id': int(tagID), 'tagValue': tagValue }
            db.tags.insert(tags_info)
print("File tags.dat imported")

with open('F:/Pycharm Projects/file_translater/comp5338/data_set/user_taggedartists-timestamps.dat') as f:
    data = f.read().split('\n')
    for line in data[1:]:
        if line.strip():
            userID, artistID, tagID, timestamp = line.split('\t')
            find_tag = db.tags.find_one({ "_id": int(tagID) })
            find_artist_name = db.artists.find_one({ "_id": int(artistID)})
            if find_artist_name is None:
                artist_name = ''
            else:
                artist_name = find_artist_name['name']
            user_artist_tag = { 'userID': int(userID), 'artistID': int(artistID), 'artist_name': artist_name, 'tagID': int(tagID), 'tagValue': find_tag['tagValue'], 'timestamp': long(timestamp) }
            db.user_artist_tag.insert(user_artist_tag)
print("File user_artist_tag imported")

db.artists.drop()
print("Collection artists dropped")

db.tags.drop()
print("Collection tags dropped")