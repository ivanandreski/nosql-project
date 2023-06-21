import pandas
from postgres.import_datasets import parse as pg_parse
from mongo.import_from_postgres import import_from_postgres
from dotenv import load_dotenv

load_dotenv()

movies_dataset = pandas.read_csv('./csv/movies_metadata.csv').head(1000)
ratings_dataset = pandas.read_csv('./csv/ratings_small.csv').head(1000)

# pg_parse(movies_dataset, ratings_dataset)
import_from_postgres()
