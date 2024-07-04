# %%
#import libs
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from datetime import date
import datetime
import os
import requests
import json
import pandas as pd

# %%
#create pyspark session
spark = SparkSession.builder\
        .master('local[1]')\
        .appName('IGDB_Collect')\
        .getOrCreate()

# %%
#get IGDB keys
path = os.getcwd()
df_keys = spark.read.csv(f'{path}\data\df_keys.csv', header= True, inferSchema= True)

# %%
#collect access url
client_id = df_keys.collect()[0][1]
secret = df_keys.collect()[1][1]
access_url = f'https://id.twitch.tv/oauth2/token?client_id={client_id}&client_secret={secret}&grant_type=client_credentials'

access_resp = requests.post(access_url)
json_access = json.loads(access_resp.text)
access_token = json_access['access_token']

# %%
#get last update at
bronze_df = spark.read.parquet(f'{path}\data\igdb\igdb_games__bronze.parquet', inferSchema= True)
df_max = bronze_df.agg(F.max('updated_at'))
max_update_at = df_max.collect()[0][0]


# %%
#collect data
offset = 0
merge_json = []
c = 0

while True:
        resp_api = requests.post('https://api.igdb.com/v4/games/', **{'headers': {'Client-ID': client_id,
                                                                'Authorization': 'Bearer ' + access_token},
                                                                'data': f'fields age_ratings,aggregated_rating,aggregated_rating_count,alternative_names,artworks,bundles,category,checksum,collection,collections,cover,created_at,dlcs,expanded_games,expansions,external_games,first_release_date,follows,forks,franchise,franchises,game_engines,game_localizations,game_modes,genres,hypes,involved_companies,keywords,language_supports,multiplayer_modes,name,parent_game,platforms,player_perspectives,ports,rating,rating_count,release_dates,remakes,remasters,screenshots,similar_games,slug,standalone_expansions,status,storyline,summary,tags,themes,total_rating,total_rating_count,updated_at,url,version_parent,version_title,videos,websites; sort updated_at desc; limit 500; offset {offset}; where updated_at > {max_update_at};'})
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
#create pandas dataframe and save to parquet
pandas_df = pd.DataFrame.from_records(merge_json)
pandas_df.to_parquet(f'data/igdb/igdb_games__{date.today()}.parquet', index=False)

# %%
#create spark dataframe
spark_df = spark.read.parquet(f'{path}\data\igdb\igdb_games__{date.today()}.parquet', inferSchema= True)

spark_df = spark_df.withColumn('extraction_date', F.current_date())

# %%
#criar lógica p/ atualizar somente novas informações no novo .parquet
