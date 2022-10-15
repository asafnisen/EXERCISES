import requests
import base64
import json
import pandas as pd

import pyodbc
import sqlalchemy as sal
from sqlalchemy import create_engine

"""
    this exercises include :     
        get data from API request spotify only one song - tiny dancer
        groupby pandas + countif + agg
        insert to sql server whith pandas when solumns diffrent columns=conv_dict
        dict Comprehension  {key:value for key, value in res.items() if key in required_fields}
        base64 connvert
        concat tadaframes
"""

server = 'LTH80051170\SQLEXPRESS'
database = 'TEST_asaf'
engine = create_engine('mssql+pyodbc://' + server + '/' + database + '?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server')


client_secret = '2c5a9e0f69234ce281b6fbce274fb718'
client_id = '763b842ed2394221950da9527fdc7d34'

creds =  f"{client_id}:{client_secret}"
creds_bs64=base64.b64encode(creds.encode())
#creds_bs64=base64.b64encode("763b842ed2394221950da9527fdc7d34:2c5a9e0f69234ce281b6fbce274fb718".encode())

url='https://accounts.spotify.com/api/token'
parameters_authorize = {'grant_type': 'client_credentials'}
headers = {"Authorization": "Basic {}".format(creds_bs64.decode()),
           "Content-Type": "application/x-www-form-urlencoded"}
response = requests.post(url, headers=headers,params=parameters_authorize)
access_token = (response.json()['access_token'])

##############################################################

#############################################################

songid="4BGJSbB5rAcg4pNzD4gfxU"
song_name="tiny Dancer"

url='https://api.spotify.com/v1/tracks/4BGJSbB5rAcg4pNzD4gfxU'
headers = {"Authorization": "Bearer {}".format(access_token)}
response = requests.get(url, headers=headers,params=parameters_authorize)
res=(response.json())

print(json.dumps(res,indent=1))

D={
    "song_name" : (res["name"]),
    "id" : (res["id"]),
    "album_name" : (res["album"]["name"]),
    "artists_name" : (res["artists"][0]["name"]),
    "popularity" : (res["popularity"])
}

df=(pd.DataFrame (D, index=[0]))
print (df)

url='https://api.spotify.com/v1/audio-analysis/4BGJSbB5rAcg4pNzD4gfxU'
headers = {"Authorization": "Bearer {}".format(access_token), 'Content-Type': 'application/json'}
response = requests.get(url, headers=headers,params=parameters_authorize)

res=(response.json())

df2= pd.DataFrame (res["track"], index=[0])
df2 = df2[["tempo","loudness"]]
df2["song_name"] = song_name

url='https://api.spotify.com/v1/audio-features/4BGJSbB5rAcg4pNzD4gfxU'
headers = {"Authorization": "Bearer {}".format(access_token), 'Content-Type': 'application/json'}
response = requests.get(url, headers=headers,params=parameters_authorize)

res=(response.json())

required_fields = ['id','danceability', 'acousticness', 'energy', 'instrumentalness', 'liveness','valence']
new_dict = {key:value for key, value in res.items() if key in required_fields}
df1= pd.DataFrame (new_dict, index=[0])

df= (pd.concat([df1, df2], axis=1))

conv_dict = {'id': 'songid'}
new_df = df.rename(columns=conv_dict)

new_df.to_sql('Spotify_song_info_2', con=engine, if_exists='append', chunksize=1000,index=False)

#INSERT INTO [Spotify_song_info_2] (danceability, energy, acousticness, instrumentalness, liveness, valence, songid, tempo, loudness, song_name) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)]
#[parameters: (0.415, 0.425, 0.396, 0.000407, 0.143, 0.281, '4BGJSbB5rAcg4pNzD4gfxU', 145.234, -11.082, 'tiny Dancer')]


