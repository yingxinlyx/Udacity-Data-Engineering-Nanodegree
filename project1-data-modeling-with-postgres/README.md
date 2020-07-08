# Data Modeling with Postgres
This is the first project of the Data Engineering Nanodegree from Udacity. 

## Purpose
    This project is using the song and log datasets to create a star schema optimized for queries on song play analysis. 

## Database schema design
### Fact Table
    songplays - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
### Dimension Tables
    users - user_id, first_name, last_name, gender, level
    songs - song_id, title, artist_id, year, duration
    artists - artist_id, name, location, latitude, longitude
    time - start_time, hour, day, week, month, year, weekday

## ETL Pipeline
### Create Tables
    Prepared `DROP` and `CREATE` queries in `sql_queries.py`, then ran `create_tables.py` to batch create all needed tables. 
### Build ETL pipeline
    There were 2 parts building the pipeline:
    The first part was to extract information from the `song` dataset and store information in `artists` and `songs`. 
    The second part was to extract information from the `log` dataset and store information in `users`, `time` and `songplays`. 