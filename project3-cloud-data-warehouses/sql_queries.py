import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
        CREATE TABLE IF NOT EXISTS staging_events
        (artist          VARCHAR,
        auth             VARCHAR,
        firstName        VARCHAR,
        gender           VARCHAR,
        itemInSession    INTEGER,
        lastName         VARCHAR,
        length           FLOAT,
        level            VARCHAR,
        location         VARCHAR,
        method           VARCHAR,
        page             VARCHAR,
        registration     FLOAT,
        sessionId        INTEGER,
        song             VARCHAR,
        status           INTEGER,
        ts               VARCHAR,
        userAgent        VARCHAR,
        userId           INTEGER)
""")

staging_songs_table_create = ("""
        CREATE TABLE IF NOT EXISTS staging_songs
        (num_songs          INTEGER,
        artist_id           VARCHAR,
        artist_latitude     FLOAT,
        artist_longitude    FLOAT,
        artist_location     VARCHAR,
        artist_name         VARCHAR,
        song_id             VARCHAR,
        title               VARCHAR,
        duration            FLOAT,
        year                INTEGER)
""")

songplay_table_create = ("""
        CREATE TABLE IF NOT EXISTS songplays
        (songplay_id    INTEGER      IDENTITY(0,1)    PRIMARY KEY,
        start_time      TIMESTAMP    NOT NULL         SORTKEY DISTKEY,
        user_id         INTEGER      NOT NULL,
        level           VARCHAR,
        song_id         VARCHAR      NOT NULL,
        artist_id       VARCHAR      NOT NULL,
        session_id      INTEGER, 
        location        VARCHAR, 
        user_agent      VARCHAR)
""")

user_table_create = ("""
        CREATE TABLE IF NOT EXISTS users
        (user_id      INTEGER    PRIMARY KEY SORTKEY, 
        first_name    VARCHAR    NOT NULL, 
        last_name     VARCHAR    NOT NULL, 
        gender        VARCHAR, 
        level         VARCHAR)
""")

song_table_create = ("""
        CREATE TABLE IF NOT EXISTS songs
        (song_id     VARCHAR    PRIMARY KEY SORTKEY, 
        title        VARCHAR    NOT NULL, 
        artist_id    VARCHAR    NOT NULL, 
        year         INTEGER, 
        duration     FLOAT      NOT NULL)
""")

artist_table_create = ("""
        CREATE TABLE IF NOT EXISTS artists
        (artist_id    VARCHAR PRIMARY KEY SORTKEY, 
        name          VARCHAR NOT NULL, 
        location      VARCHAR, 
        latitude      FLOAT, 
        longitude     FLOAT)
""")

time_table_create = ("""
        CREATE TABLE IF NOT EXISTS time
        (start_time    TIMESTAMP    PRIMARY KEY SORTKEY DISTKEY, 
        hour           INTEGER, 
        day            INTEGER, 
        week           INTEGER, 
        month          INTEGER, 
        year           INTEGER, 
        weekday        VARCHAR)
""")

# STAGING TABLES

staging_events_copy = ("""
        copy staging_events from {}
        credentials 'aws_iam_role={}'
        region 'us-west-2' format as JSON {}
        timeformat as 'epochmillisecs';
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
        copy staging_songs from {}
        credentials 'aws_iam_role={}'
        region 'us-west-2' format as JSON 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
        INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT  DISTINCT TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second' AS start_time, 
                e.userId       AS user_id, 
                e.level        AS level, 
                s.song_id      AS song_id, 
                s.artist_id    AS artist_id, 
                e.sessionId    AS session_id, 
                e.location     AS location, 
                e.userAgent    AS user_agent
        FROM staging_events e
        JOIN staging_songs  s  ON (e.song = s.title AND e.artist = s.artist_name)
        AND e.page = 'NextSong'
""")

user_table_insert = ("""
        INSERT INTO users (user_id, first_name, last_name, gender, level)
        SELECT  DISTINCT(userId) AS user_id,
                firstName        AS first_name,
                lastName         AS last_name,
                gender,
                level
        FROM staging_events
        WHERE user_id IS NOT NULL
        AND page = 'NextSong';
""")

song_table_insert = ("""
        INSERT INTO songs (song_id, title, artist_id, year, duration)
        SELECT  DISTINCT(song_id) AS song_id,
                title,
                artist_id,
                year,
                duration
        FROM staging_songs
        WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
        INSERT INTO artists (artist_id, name, location, latitude, longitude)
        SELECT  DISTINCT(artist_id) AS artist_id,
                artist_name         AS name,
                artist_location     AS location,
                artist_latitude     AS latitude,
                artist_longitude    AS longitude
        FROM staging_songs
        WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
        INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT  ts                       AS start_time,
                EXTRACT(hr FROM ts)      AS hour,
                EXTRACT(d FROM ts)       AS day,
                EXTRACT(w FROM ts)       AS week,
                EXTRACT(mon FROM ts)     AS month,
                EXTRACT(yr FROM ts)      AS year,
                EXTRACT(weekday FROM ts) AS weekday
        FROM (SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second'  as ts 
             FROM staging_events
             WHERE page = 'NextSong') tmp;        
""")

# ANALYTIC QUREY 
staging_events_analytic = ("""SELECT count(*) from staging_events;""")
staging_songs_analytic = ("""SELECT count(*) from staging_songs;""")
songplays_analytic = ("""SELECT count(*) from songplays;""")
users_analytic = ("""SELECT count(*) from users;""")
songs_analytic = ("""SELECT count(*) from songs;""")
artists_analytic = ("""SELECT count(*) from artists;""")
time_analytic = ("""SELECT count(*) from time;""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, artist_table_create, song_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, artist_table_drop, song_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, artist_table_insert, song_table_insert, time_table_insert]

analytic_queries = [staging_events_analytic, staging_songs_analytic, songplays_analytic, users_analytic, songs_analytic, artists_analytic, time_analytic]