# %%

import requests
import json
import pandas as pd
import sqlalchemy
import sqlite3

CLIENT_ID = "ENTER_CLIENT_ID_HERE"
CLIENT_SECRET = "ENTER_CLIENT_SECRET_HERE"
url='https://accounts.spotify.com/api/token'
body_params = {'grant_type' : 'client_credentials'}
response = requests.post(url, data = body_params, auth = (CLIENT_ID, CLIENT_SECRET))
token_raw = json.loads(response.text)
token = token_raw["access_token"]

USER_ID = "ENTER_SPOTIFY_USER_ID_HERE" #My spotify username
TOKEN = token #spotify API token
headers = {
    "Accept" : "application/json",
    "Content-Type" : "application/json",
    "Authorization" : "Bearer {token}".format(token=TOKEN)
}

def check_if_valid_data(df) -> bool:

    # Check if dataframe is empty:
    if df.empty:
        print("No data downloaded.")
        return False

    # Primary key check:
    if pd.Series(df["track_id"]).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated.")

    # Check for nulls:
    if df.isnull().values.any():
        raise Exception("Null values found.")

    return True


if __name__ == "__main__":
    
    playlist_id = "37i9dQZEVXbLp5XoPON0wI" # Spotify playlist ID for USA top 50 songs

    r = requests.get(f"https://api.spotify.com/v1/playlists/{playlist_id}".format(playlist_id), headers=headers) # Request to get data on a playlist id

    # Parse out the nested data:
    data = r.json()
    tracks = data['tracks']
    tracks_items = tracks['items']
    track_data = [i['track'] for i in tracks_items] # Getting a specific "column" of values from a list of dicts

    # Extract data of interest into lists:
    track_name = []
    track_id = []
    track_popularity = []
    track_duration_ms = []

    for item in track_data:
        track_name.append(item['name'])
        track_id.append(item['id'])
        track_popularity.append(item['popularity'])
        track_duration_ms.append(item['duration_ms'])

    # Prepare dictionary in order to turn it into a dataframe
    track_dict = {
        "track_id": track_id
    }

    track_df = pd.DataFrame(track_dict, columns = ["track_id"])


    # Extract more data of interest:
    danceability = []
    energy = []
    key = []
    loudness = []
    mode = []
    speechiness = []
    acousticness = []
    instrumentalness = []
    liveness = []
    valence = []
    tempo = []
    time_signature = []

    for t in track_df['track_id']:

        r2 = requests.get(f"https://api.spotify.com/v1/audio-features/{t}".format(t), headers = headers) # Request for getting audio features for a track id

        raw_audio_features = r2.json()

        danceability.append(raw_audio_features['danceability'])
        energy.append(raw_audio_features['energy'])
        key.append(raw_audio_features['key'])
        loudness.append(raw_audio_features['loudness'])
        mode.append(raw_audio_features['mode'])
        speechiness.append(raw_audio_features['speechiness'])
        acousticness.append(raw_audio_features['acousticness'])
        instrumentalness.append(raw_audio_features['instrumentalness'])
        liveness.append(raw_audio_features['liveness'])
        valence.append(raw_audio_features['valence'])
        tempo.append(raw_audio_features['tempo'])
        time_signature.append(raw_audio_features['time_signature'])

    # Prepare a dictionary from lists in order to turn it into a dataframe
    track_dict = {
        "track_name": track_name,
        "track_id": track_id,
        "track_popularity": track_popularity,
        "track_duration_ms": track_duration_ms,
        "danceability": danceability,
        "energy": energy,
        "key": key,
        "loudness": loudness,
        "mode": mode,
        "speechiness": speechiness,
        "acousticness": acousticness,
        "instrumentalness": instrumentalness,
        "liveness": liveness,
        "valence": valence,
        "tempo": tempo,
        "time_signature": time_signature
    }

    track_df = pd.DataFrame(track_dict, columns = ["track_name", "track_id", "track_popularity", "track_duration_ms", "danceability", "energy", "key", "loudness", "mode", "speechiness", "acousticness", "instrumentalness", "liveness", "valence", "tempo", "time_signature"])


    # Validate data:
    if check_if_valid_data(track_df):
        print("Data is valid. Proceeding to loading.")

    # Open database connection for loading:
    engine = sqlalchemy.create_engine("sqlite:///spotify.sqlite") # Opening up a connection and database on local machine's SQLite
    conn = sqlite3.connect('spotify.sqlite') # Connecting to the database
    cursor = conn.cursor()

    # Write query for loading data into "tracks" table
    sql_query = """
        CREATE TABLE IF NOT EXISTS tracks(
            track_name VARCHAR(200),
            track_id VARCHAR(200),
            track_popularity VARCHAR(2),
            track_duration_ms VARCHAR(15),
            CONSTRAINT primary_key_constraint PRIMARY KEY (track_id)
        )
    """ 

    cursor.execute(sql_query)
    print("Opened database successfully.")

    try:
        track_df.to_sql("tracks", engine, index=False, if_exists='replace')
        print("Data loaded successfully.")
    finally:
        conn.close()
        print("Database closed successfully.")
    

    


# %%
