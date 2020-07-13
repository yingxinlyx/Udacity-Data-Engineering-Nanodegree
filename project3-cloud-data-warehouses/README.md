# Data Warehouse ETL Process
This is the third project of the Data Engineering Nanodegree from Udacity.

## 1. Purpose
Using the song and event datasets, this project created a star schema optimized for queries on song play analysis.

## 2. Data warehouse schema
### 1) Fact Table
songplays - songplay_id, start_time(SORTKEY DISTKEY), user_id, level, song_id, artist_id, session_id, location, user_agent
### 2) Dimension Tables
users - user_id(SORTKEY), first_name, last_name, gender, level
songs - song_id(SORTKEY), title, artist_id, year, duration
artists - artist_id(SORTKEY), name, location, latitude, longitude
time - start_time(SORTKEY DISTKEY), hour, day, week, month, year, weekday

## 3. ETL pipeline
### 1) Create staging tables and data warehouse tables
Prepared `DROP` and `CREATE` queries in `sql_queries.py`, then ran `create_tables.py` to create all needed tables. 
### 2) Build ETL pipeline
There were 2 parts building the pipeline: 
The first part was to stage data from the `LOG_DATA` and `SONG_DATA` stored in S3 
The second part was to insert information from the staging tables in `songplays`, `users`, `artists`, `songs` and `time`.

## 4. Analytics
Simply checked how many rows in each table:
staging_events: 8056
staging_songs: 14896
songplays: 333
users: 104
songs: 14896
artists: 10025
time: 6813