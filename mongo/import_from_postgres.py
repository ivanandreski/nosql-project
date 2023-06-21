from pymongo import MongoClient
import json

def import_from_postgres():
    client = MongoClient('127.0.0.1', 27017, username='admin', password='admin')
    db = client['movies']
    movies_table = db['movies']

    f = open('./pg_json/genres_202306212000.json')
    genres_dataset = json.load(f)
    f = open('./pg_json/movies_202306212000.json')
    movies_dataset = json.load(f)
    f = open('./pg_json/countries_202306212000.json')
    countries_dataset = json.load(f)
    f = open('./pg_json/movie_genres_202306212000.json')
    movie_genres_dataset = json.load(f)
    f = open('./pg_json/movie_production_companies_202306212000.json')
    movie_production_companies_dataset = json.load(f)
    f = open('./pg_json/movie_production_countries_202306212000.json')
    movie_production_countries_dataset = json.load(f)
    f = open('./pg_json/production_companies_202306212000.json')
    production_companies_dataset = json.load(f)
    f = open('./pg_json/ratings_202306212000.json')
    ratings_dataset = json.load(f)

    for movie in movies_dataset['movies']:
        genre_ids = []
        for x in movie_genres_dataset['movie_genres']:
            if x['movie_id'] == movie['id']:
                genre_ids.append(x['genre_id'])
        genres = []
        for x in genres_dataset['genres']:
            if x['id'] in genre_ids:
                genres.append(x['name'])

        country_ids = []
        for x in movie_production_countries_dataset['movie_production_countries']:
            if x['movie_id'] == movie['id']:
                country_ids.append(x['country_id'])
        countries = []
        for x in countries_dataset['countries']:
            if x['iso_3166_1'] in country_ids:
                countries.append(x['name'])

        company_ids = []
        for x in movie_production_companies_dataset['movie_production_companies']:
            if x['movie_id'] == movie['id']:
                company_ids.append(x['production_company_id'])
        companies = []
        for x in production_companies_dataset['production_companies']:
            if x['id'] in company_ids:
                companies.append(x['name'])

        movies_table.insert_one({
            'id': movie['id'],
            "imdb_id" : movie['imdb_id'],
            "original_title" : movie['original_title'],
            "overview" : movie['overview'],
            "original_language" : movie['original_language'],
            "budget" : movie['budget'],
            "homepage" : movie['homepage'],
            "release_date" : movie['release_date'],
            "status" : movie['status'],
            "revenue" : movie['revenue'],
            "runtime" : movie['runtime'],
            "genres": genres,
            "production_countries": countries,
            "production_companies": companies
        })
    
    ratings_table = db['ratings']
    for rating in ratings_dataset['ratings']:
        ratings_table.insert_one({
            'movie_id': rating['movie_id'],
            "user_id" : rating['user_id'],
            "rating" : rating['rating'],
        })