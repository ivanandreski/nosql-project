import pandas
from postgres.movies_metadata import parse_movies as pg_parse_movies

movies_dataset = pandas.read_csv('./csv/movies_metadata.csv')
credits_dataset = pandas.read_csv('./csv/credits.csv')
keywords_dataset = pandas.read_csv('./csv/keywords.csv')
ratings_dataset = pandas.read_csv('./csv/ratings.csv')

pg_parse_movies(movies_dataset)