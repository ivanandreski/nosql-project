def create_tables(conn):
    cursor = conn.cursor()

    # create movies table
    create_movies_query = f"""
        drop table if exists movie_production_countries;
        drop table if exists movie_production_companies;
        drop table if exists movie_genres;
        drop table if exists movies;
        drop table if exists genres;
        drop table if exists countries;
        drop table if exists production_companies;

        create table genres (
            id bigInt not null,
            name varchar(255),
            
            primary key(id)
        );

        create table countries (
            iso_3166_1 varchar(5) not null,
            name varchar(255),
            
            primary key(iso_3166_1)
        );

        create table production_companies (
            id bigInt not null,
            name varchar(255),
            
            primary key(id)
        );
                
        create table movies (
            id bigInt not null,
            imdb_id varchar(255),
            original_title varchar(255),
            overview varchar(255),
            original_language varchar(5),
            budget int,
            homepage varchar(255),
            release_date date,
            status varchar(255),
            revenue float,
            runtime float,
            
            primary key(id)
        );

        create table movie_genres (
            movie_id bigInt not null,
            genre_id bigInt not null,
            
            primary key(movie_id, genre_id),
            foreign key(movie_id) references movies(id),
            foreign key(genre_id) references genres(id)
        );

        create table movie_production_companies (
            movie_id bigInt not null,
            production_company_id bigInt not null,
            
            primary key(movie_id, production_company_id),
            foreign key(movie_id) references movies(id),
            foreign key(production_company_id) references production_companies(id)
        );

        create table movie_production_countries (
            movie_id bigInt not null,
            country_id varchar(5) not null,
            
            primary key(movie_id, country_id),
            foreign key(movie_id) references movies(id),
            foreign key(country_id) references countries(iso_3166_1)
        );
    """

    cursor.execute(create_movies_query)