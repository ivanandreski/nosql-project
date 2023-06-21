# dropped adult column, always false
# dropped belongs_to_collection column, not important
# budget: number
# genres json array of objects  [{'id': 16, 'name': 'Animation'}, {'id': 35, '...
# homepage url string
# id number
# imdb_id string
# original_language string ex: en
# original_title string
# overview description string
# popularity float ?? wtf is this
# removed poster_path, its a local path not  url
# production_companies json array [{'name': 'Pixar Animation Studios', 'id': 3}]
# production_countries json_array [{'iso_3166_1': 'US', 'name': 'United States o...
# release_date date in format 1995-10-30
# revenue float, probably in dollars
# runtime float in minutes
# dropped spoken_languages [{'iso_639_1': 'en', 'name': 'English'}]
# status ex: Released
# dropped tagline, mostly NaN
# dropped video, mostly empty
# two vote columns dropped, we can get this from the ratings csv

import psycopg2
import json
from postgres.pg_tables import create_tables

def parse_movies(dataset):
    conn = psycopg2.connect(database="movies",
                            host="127.0.0.1",
                            user="postgres",
                            password="postgres",
                            port="5432")
    
    # create_tables(conn)

    genre_ids = []
    production_companies_ids = []
    production_countries_ids = []

    for index, row in dataset.iterrows():
        movie_id = row['id']
        imdb_id = row['imdb_id']
        original_title = row['original_title']
        overview = row['overview']
        original_language = row['original_language']
        budget = row['budget']
        homepage = row['homepage']
        release_date = row['release_date']
        status = row['status']
        revenue = row['revenue']
        runtime = row['runtime']

        genres = json.loads(row['genres'].replace("\'", "\""))
        production_companies = json.loads(row['production_companies'].replace("\'", "\""))
        production_countries = json.loads(row['production_countries'].replace("\'", "\""))
    
        # TODO: add movie to db

        for genre in genres:
            if genre['id'] not in genre_ids:
                # TODO: insert genre into genres table and add the id to genre_ids, add new row in movie_genres

        # TODO: repeat proces for production_companies and production_countries

    