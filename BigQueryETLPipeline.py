import os
import pandas as pd
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/burug/Downloads/sylvan-byway-464104-m3-f83c3238d888.json"

client = bigquery.Client()


query = """
CREATE TABLE IF NOT EXISTS sylvan-byway-464104-m3.miniProject.Directors
(director_id INT64, Director STRING)
"""
job = client.query(query)

df = pd.read_csv("C:/Users/burug/Downloads/movie_genre_classification_final.csv")
df.drop_duplicates()

print(df.head(5))
#adding id for director
df_directors = df[['Director']].drop_duplicates().reset_index(drop=True)
df_directors['director_id'] = df_directors.index + 1
df = df.merge(df_directors, on='Director', how='left')
query = """
CREATE TABLE IF NOT EXISTS sylvan-byway-464104-m3.miniProject.Languages
(director_id INT64, Director STRING)
"""
job = client.query(query)
table_id = "sylvan-byway-464104-m3.miniProject.Directors"
df_directors = df[['director_id', 'Director']].drop_duplicates().reset_index(drop=True)
job_config = bigquery.LoadJobConfig(write_disposition = "WRITE_TRUNCATE", autodetect = True)
job = client.load_table_from_dataframe(df_directors, table_id, job_config = job_config)

#adding id for language
df_language = df[['Language']].drop_duplicates().reset_index(drop=True)
df_language['language_id'] = df_language.index + 1
df = df.merge(df_language, on='Language', how='left')
query = """
CREATE TABLE IF NOT EXISTS `sylvan-byway-464104-m3.miniProject.Languages` 
(language_id int64, Language string)
"""
job = client.query(query)
table_id = "sylvan-byway-464104-m3.miniProject.Languages"
df_language = df[['language_id', 'Language']].drop_duplicates().reset_index(drop=True)
job_config = bigquery.LoadJobConfig(write_disposition = "WRITE_TRUNCATE", autodetect = True)
job = client.load_table_from_dataframe(df_language, table_id, job_config = job_config)

#adding id for genre
df_genre = df[['Genre']].drop_duplicates().reset_index(drop=True)
df_genre['genre_id'] = df_genre.index + 1
df = df.merge(df_genre, on='Genre', how='left')
query = """
CREATE TABLE IF NOT EXISTS `sylvan-byway-464104-m3.miniProject.Genres` 
(genre_id int64, Genre string)
"""
job = client.query(query)
table_id = "sylvan-byway-464104-m3.miniProject.Genres"
df_genre = df[['genre_id', 'Genre']].drop_duplicates().reset_index(drop=True)
job_config = bigquery.LoadJobConfig(write_disposition = "WRITE_TRUNCATE", autodetect = True)
job = client.load_table_from_dataframe(df_genre, table_id, job_config = job_config)

#adding id for lead_Actor
df_lead_actor = df[['Lead_Actor']].drop_duplicates().reset_index(drop=True)
df_lead_actor['lead_actor_id'] = df_lead_actor.index + 1
df = df.merge(df_lead_actor, on='Lead_Actor', how='left')
query = """
CREATE TABLE IF NOT EXISTS `sylvan-byway-464104-m3.miniProject.LeadActors` 
(lead_actor_id int64, Lead_Actor string)
"""
job = client.query(query)
table_id = "sylvan-byway-464104-m3.miniProject.LeadActors"
df_lead_actor = df[['lead_actor_id', 'Lead_Actor']].drop_duplicates().reset_index(drop=True)
job_config = bigquery.LoadJobConfig(write_disposition = "WRITE_TRUNCATE", autodetect = True)
job = client.load_table_from_dataframe(df_lead_actor, table_id, job_config = job_config)

#creating fact table
query = """
CREATE TABLE IF NOT EXISTS `sylvan-byway-464104-m3.miniProject.MovieData` 
(Title STRING, Year int64, Duration int64, Rating Float64, Votes int64,
Budget_USD int64, BoxOffice_USD int64, Content_Rating float64, Num_Awards int64, 
Critical_Rating float64, director_id int64, language_id int64, genre_id int64, 
lead_actor_id int64)
"""
job = client.query(query)
df_movie_data = df[['Title', 'Year', 'Duration', 'Rating', 'Votes', 'Budget_USD', 
                    'BoxOffice_USD', 'Content_Rating', 'Num_Awards', 'Critic_Reviews', 'director_id',
                    'language_id', 'genre_id', 'lead_actor_id']]
table_id = "sylvan-byway-464104-m3.miniProject.MovieData"
job_config = bigquery.LoadJobConfig(write_disposition = "WRITE_TRUNCATE", autodetect = True)
job = client.load_table_from_dataframe(df_movie_data, table_id, job_config = job_config)
print(df.head(5))


# print(df.head(5))
print("END")