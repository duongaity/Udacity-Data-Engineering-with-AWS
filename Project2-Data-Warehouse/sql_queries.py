import configparser

# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABle IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE  IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        event_id            BIGINT IDENTITY(0,1),
        artist              VARCHAR,
        auth                VARCHAR,
        first_name          VARCHAR,
        gender              VARCHAR,
        item_in_session     VARCHAR,
        last_name           VARCHAR,
        length              VARCHAR,
        level               VARCHAR,
        location            VARCHAR,
        method              VARCHAR,
        page                VARCHAR,
        registration        VARCHAR,
        session_id          INTEGER,
        song                VARCHAR,
        status              INTEGER,
        ts                  BIGINT,
        user_agent          VARCHAR,
        user_id             INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs           INTEGER,
        artist_id           VARCHAR,
        artist_latitude     FLOAT,
        artist_longitude    FLOAT,
        artist_location     TEXT,
        artist_name         VARCHAR,
        song_id             VARCHAR,
        title               VARCHAR,
        duration            FLOAT,
        year                INTEGER
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id         INTEGER IDENTITY(0,1) NOT NULL PRIMARY KEY,
        start_time          TIMESTAMP,
        user_id             INTEGER,
        level               VARCHAR,
        song_id             VARCHAR,
        artist_id           VARCHAR,
        session_id          INTEGER,
        location            TEXT,
        user_agent          TEXT
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id             INTEGER NOT NULL PRIMARY KEY,
        first_name          VARCHAR,
        last_name           VARCHAR,
        gender              CHAR(1),
        level               VARCHAR
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id             VARCHAR NOT NULL PRIMARY KEY,
        title               VARCHAR,
        artist_id           VARCHAR,
        year                INTEGER,
        duration            FLOAT
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id           VARCHAR NOT NULL PRIMARY KEY,
        name                VARCHAR,
        location            TEXT,
        latitude            FLOAT,
        longitude           FLOAT
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time          TIMESTAMP NOT NULL PRIMARY KEY,
        hour                INTEGER,
        day                 INTEGER,
        week                INTEGER,
        month               INTEGER,
        year                INTEGER,
        weekday             VARCHAR
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM {}
    iam_role {}
    FORMAT AS json {}
    REGION 'us-west-2';
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    iam_role {}
    FORMAT AS json 'auto'
    REGION 'us-west-2';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT TIMESTAMP 'epoch' + sp.ts/1000 * INTERVAL '1 second' AS start_time,
            sp.user_id            AS user_id,
            sp.level              AS level,
            ss.song_id            AS song_id,
            ss.artist_id          AS artist_id,
            sp.session_id         AS session_id,
            sp.location           AS location,
            sp.user_agent         AS user_agent
    FROM songplays AS sp
    JOIN staging_songs AS ss ON (sp.song = ss.title AND sp.artist = ss.artist_name)
    WHERE sp.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT sp.user_id    AS user_id,
            sp.first_name         AS first_name,
            sp.last_name          AS last_name,
            sp.gender             AS gender,
            sp.level              AS level
    FROM songplays AS sp
    WHERE sp.page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT ss.song_id    AS song_id,
            ss.title              AS title,
            ss.artist_id          AS artist_id,
            ss.year               AS year,
            ss.duration           AS duration
    FROM staging_songs AS ss;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT ss.artist_id      AS artist_id,
            ss.artist_name            AS name,
            ss.artist_location        AS location,
            ss.artist_latitude        AS latitude,
            ss.artist_longitude       AS longitude
    FROM staging_songs AS ss;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT TIMESTAMP 'epoch' + sp.ts/1000 * INTERVAL '1 second'  AS start_time,
            EXTRACT(hour FROM start_time)    AS hour,
            EXTRACT(day FROM start_time)     AS day,
            EXTRACT(week FROM start_time)    AS week,
            EXTRACT(month FROM start_time)   AS month,
            EXTRACT(year FROM start_time)    AS year,
            EXTRACT(week FROM start_time)    AS weekday
    FROM songplays AS sp
    WHERE sp.page = 'NextSong';
""")

# ANALYTIC
select_number_staging_events = "SELECT COUNT(*) FROM staging_events"
select_number_staging_songs = "SELECT COUNT(*) FROM staging_songs"
select_number_songplays = "SELECT COUNT(*) FROM songplays"
select_number_users = "SELECT COUNT(*) FROM users"
select_number_songs = "SELECT COUNT(*) FROM songs"
select_number_artists = "SELECT COUNT(*) FROM artists"
select_number_time = "SELECT COUNT(*) FROM time"

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
select_number_rows = [select_number_staging_events,select_number_staging_songs, select_number_songplays, select_number_users, select_number_songs, select_number_artists, select_number_time]
