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
# spoken_languages [{'iso_639_1': 'en', 'name': 'English'}]
# status ex: Released
# dropped tagline, mostly NaN
# two vote columns dropped, we can get this from the ratings csv