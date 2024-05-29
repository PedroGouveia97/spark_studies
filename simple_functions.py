# %%
#import libs
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

# %%
#create pyspark session
spark = SparkSession.builder\
        .master('local')\
        .appName('sparkcolab')\
        .getOrCreate()

# %%
#import dataset
path = os.getcwd()
df = spark.read.csv(f'{path}\data\movies.csv', header= True, inferSchema= True)
df.printSchema()

# %%
#select only wanted columns
df_movie = df.select('name', 'year', 'genre', 'directors_name', 'votes')

# %%
#modify column type
df_movie = df_movie.withColumn('votes_int', df_movie['votes'].cast('int')).drop('votes')
df_movie.show()

# %%
#filter by votes
df_movie.filter(df_movie.votes_int > 500).show()

# %%
#get only the most voted
df_max = df_movie.agg(F.max('votes_int'))
max_value = df_max.collect()[0][0]
df_movie.filter(df_movie.votes_int == max_value).show()

# %%
#count cetegories by order
df_movie.groupBy('genre').count().orderBy(F.col('count').desc()).show(truncate=False)

# %%
#in filter
df_movie.filter(df_movie.genre.isin('Sport', 'Music')).show()

# %%
#not in filter
df_movie.filter(~df_movie.genre.isin('Sport', 'Music')).show()

# %%
#using sql in pyspakr
df_movie.createOrReplaceTempView('movies')
spark.sql('select genre, count(*) as qty from movies group by 1 order by 2 desc').show(truncate=False)

# %%
#simulating a join necessity 
df_feedback = df.select('id', 'rating', 'votes')
df_name = df.select('id', 'name')

# %%
#casting to numeric values
df_feedback = df_feedback.withColumn('votes_int', F.regexp_replace('votes', ',', '')).drop('votes')
df_feedback = df_feedback.withColumn('rating', df_feedback['rating'].cast('float'))

# %%
df_full =       df_name.join(df_feedback, df_name.id == df_feedback.id, 'left')\
                        .select(df_name.name, df_feedback.rating, df_feedback.votes_int)

# %%
df_full.show() 
# %%
