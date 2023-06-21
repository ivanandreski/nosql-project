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
import re
from postgres.pg_tables import create_tables
import os
import math

def parse(movie_dataset, ratings_dataset):
    PG_DATABASE = os.getenv('PG_DATABASE')
    PG_HOST = os.getenv('PG_HOST')
    PG_USER = os.getenv('PG_USER')
    PG_PASSWORD = os.getenv('PG_PASSWORD')
    PG_PORT = os.getenv('PG_PORT')

    conn = psycopg2.connect(database=PG_DATABASE,
                            host=PG_HOST,
                            user=PG_USER,
                            password=PG_PASSWORD,
                            port=PG_PORT)
    cursor = conn.cursor()

    create_tables(cursor)

    movie_ids = []

    genre_ids = []
    production_companies_ids = []
    production_countries_ids = []

    for index, row in movie_dataset.iterrows():
        movie_id = row['id']

        if movie_id in movie_ids:
            continue

        movie_ids.append(movie_id)

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

        if math.isnan(runtime):
            runtime = 0.0
        if not isinstance(release_date, str) and math.isnan(release_date):
            release_date = '1900-01-01'

        pattern = r'([\'\"][^"]+[\'\"])\s*:\s*([\'\"]?[^\']+[\'\"]?)'

        try:
            genres = json.loads(re.sub(pattern, lambda m: m.group(1).replace("'", '"') + ':' + m.group(2).replace("'", '"'), row['genres']))
            production_companies = json.loads(re.sub(pattern, lambda m: m.group(1).replace("'", '"') + ':' + m.group(2).replace("'", '"'), row['production_companies']))
            production_countries = json.loads(re.sub(pattern, lambda m: m.group(1).replace("'", '"') + ':' + m.group(2).replace("'", '"'), row['production_countries']))
        except:
            continue

        
        original_title_escaped = str(original_title).replace("'", "''")
        overview_escaped = str(overview).replace("'", "''")
        homepage_escaped = str(homepage).replace("'", "''")

        insert_movie_query = f"""
            insert into movies
            values (
                {movie_id},
                '{imdb_id}',
                '{original_title_escaped}',
                '{overview_escaped}',
                '{original_language}',
                {budget},
                '{homepage_escaped}',
                '{release_date}',
                '{status}',
                {revenue},
                {runtime}
            )
        """

        cursor.execute(insert_movie_query)

        for genre in genres:
            if genre['id'] not in genre_ids:
                insert_genre_query = f"""
                    insert into genres
                    values (
                        {genre['id']},
                        '{genre['name']}'
                    )
                """
                cursor.execute(insert_genre_query)
                genre_ids.append(genre['id'])

            insert_movie_genre_query = f"""
                insert into movie_genres
                values (
                    {movie_id},
                    {genre['id']}
                )
            """
            cursor.execute(insert_movie_genre_query)
        for production_company in production_companies:
            if production_company['id'] not in production_companies_ids:
                insert_production_company_query = f"""
                    insert into production_companies
                    values (
                        {production_company['id']},
                        '{production_company['name']}'
                    )
                """
                cursor.execute(insert_production_company_query)
                production_companies_ids.append(production_company['id'])

            insert_movie_production_company_query = f"""
                insert into movie_production_companies
                values (
                    {movie_id},
                    {production_company['id']}
                )
            """
            cursor.execute(insert_movie_production_company_query)
        for production_country in production_countries:
            if production_country['iso_3166_1'] not in production_countries_ids:
                insert_production_country_query = f"""
                    insert into countries
                    values (
                        '{production_country['iso_3166_1']}',
                        '{production_country['name']}'
                    )
                """
                cursor.execute(insert_production_country_query)
                production_countries_ids.append(production_country['iso_3166_1'])

            insert_movie_production_country_query = f"""
                insert into movie_production_countries
                values (
                    {movie_id},
                    '{production_country['iso_3166_1']}'
                )
            """
            cursor.execute(insert_movie_production_country_query)

    insert_movie_rating_query = 'insert into ratings values'
    for index, row in ratings_dataset.iterrows():
        user_id = row['userId']
        movie_id = row['movieId']
        rating = float(row['rating'])

        insert_movie_rating_query = f""" 
            insert into ratings
            values (
                {movie_id},
                {user_id},
                {rating}
            )
        """
        cursor.execute(insert_movie_rating_query)
    
    conn.commit()