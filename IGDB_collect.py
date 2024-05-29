# %%
#import libs
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os
import requests
import json

# %%
#create pyspark session
spark = SparkSession.builder\
        .master('local')\
        .appName('IGDB_Collect')\
        .getOrCreate()
# %%
#get IGDB keys
path = os.getcwd()
df_keys = spark.read.csv(f'{path}\data\df_keys.csv', header= True, inferSchema= True)

# %%
client_id = df_keys.collect()[0][1]
secret = df_keys.collect()[1][1]
access_url = f'https://id.twitch.tv/oauth2/token?client_id={client_id}&client_secret={secret}&grant_type=client_credentials'

access_resp = requests.post(access_url)
# %%
json_access = json.loads(access_resp.text)
access_token = json_access['access_token']
# %%
offset = 0
merge_json = []

while True:
        resp_api = requests.post('https://api.igdb.com/v4/games/', **{'headers': {'Client-ID': client_id,
                                                                'Authorization': 'Bearer ' + access_token},
                                                               'data': f'fields age_ratings,aggregated_rating,aggregated_rating_count,alternative_names,artworks,bundles,category,checksum,collection,collections,cover,created_at,dlcs,expanded_games,expansions,external_games,first_release_date,follows,forks,franchise,franchises,game_engines,game_localizations,game_modes,genres,hypes,involved_companies,keywords,language_supports,multiplayer_modes,name,parent_game,platforms,player_perspectives,ports,rating,rating_count,release_dates,remakes,remasters,screenshots,similar_games,slug,standalone_expansions,status,storyline,summary,tags,themes,total_rating,total_rating_count,updated_at,url,version_parent,version_title,videos,websites; limit 500; offset {offset};'})
        offset += 500
        status_code = resp_api.status_code
        json_api = json.loads(resp_api.text)
        n_games = len(json_api)
        if n_games > 0:
                for row in json_api:
                        merge_json.append(row)
        else:
                break
# %%

# %%
# File name
file_name = f'{path}\data\igdb_output.json'

# Step 1: Read existing data
if os.path.exists(file_name):
    with open(file_name, 'r') as file:
        existing_data = json.load(file)
else:
    existing_data = []

# Step 2: Append new data
existing_data.extend(merge_json)

# Step 3: Write updated list to file
with open(file_name, 'w') as file:
    json.dump(existing_data, file, indent=4)

print(f"New JSON objects appended to {file_name}")

# %%
#query p/ pegar apenas novos inputs da API
#criar dataframe com dados salvos
#salvar dataframe em .parquet com a data. ex: igdb_games__2024_05_29.parquet
#automatizar p/ script rodar 1x por semana