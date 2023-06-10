import configparser

# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABlE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE  IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender CHAR(1),
        itemInSession INTEGER,
        lastName VARCHAR,
        length FLOAT,
        level VARCHAR,
        location TEXT,
        method VARCHAR,
        page VARCHAR,
        registration FLOAT,
        sessionId INTEGER,
        song VARCHAR,
        status INTEGER,
        ts BIGINT,
        userAgent TEXT,
        userId INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs INTEGER,
        artist_id VARCHAR,
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location TEXT,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR,
        duration FLOAT,
        year INTEGER
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INTEGER IDENTITY(0,1) NOT NULL PRIMARY KEY,
        start_time TIMESTAMP NOT NULL,
        user_id INTEGER NOT NULL,
        level VARCHAR,
        song_id VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        session_id INTEGER NOT NULL,
        location TEXT NOT NULL,
        user_agent TEXT NOT NULL
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER NOT NULL PRIMARY KEY,
        first_name VARCHAR NOT NULL,
        last_name VARCHAR NOT NULL,
        gender CHAR(1) NOT NULL,
        level VARCHAR NOT NULL
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR NOT NULL PRIMARY KEY,
        title VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        year INTEGER NOT NULL,
        duration FLOAT NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR NOT NULL PRIMARY KEY,
        name VARCHAR NOT NULL,
        location TEXT,
        latitude FLOAT NOT NULL,
        longitude FLOAT NOT NULL
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP NOT NULL PRIMARY KEY,
        hour INTEGER NOT NULL,
        day INTEGER NOT NULL,
        week INTEGER NOT NULL,
        month INTEGER NOT NULL,
        year INTEGER NOT NULL,
        weekday VARCHAR NOT NULL
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events 
    from {} 
    credentials {}
    format as json {} 
    compupdate off 
    region {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'], config['AWS']['REGION'])

staging_songs_copy = ("""
    copy staging_songs 
    from {} 
    credentials {}
    format as json 'auto' 
    compupdate off 
    region {};
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'], config['AWS']['REGION'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (
        start_time, 
        user_id, 
        level,
        song_id, 
        artist_id, 
        session_id, 
        location, 
        user_agent
    ) 
    SELECT 
        timestamp 'epoch' + se.ts/1000 * interval '1 second', 
        se.userId, 
        se.level, 
        ss.song_id, 
        ss.artist_id, 
        se.sessionId, 
        se.location, 
        se.userAgent
    FROM staging_events se 
    JOIN staging_songs ss ON (se.song = ss.title AND se.artist = ss.artist_name)
    WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (
        user_id, 
        first_name, 
        last_name, 
        gender, 
        level
    ) 
    SELECT 
        DISTINCT se.userId, 
        se.firstName, 
        se.lastName, 
        se.gender, 
        se.level
    FROM staging_events se
    WHERE se.page = 'NextSong' AND se.userId IS NOT NULL
""")

song_table_insert = ("""
    INSERT INTO songs (
        song_id, 
        title, 
        artist_id, 
        year, 
        duration
    )
    SELECT 
        DISTINCT ss.song_id, 
        ss.title, 
        ss.artist_id, 
        ss.year, 
        ss.duration
    FROM staging_songs ss
    WHERE ss.song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artists (
        artist_id, 
        name, 
        location, 
        latitude, 
        longitude
    )
    SELECT 
        DISTINCT ss.artist_id, 
        ss.artist_name, 
        ss.artist_location, 
        ss.artist_latitude, 
        ss.artist_longitude
    FROM staging_songs ss
    WHERE ss.artist_id IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO time (
        start_time, 
        hour, 
        day, 
        week, 
        month, 
        year, 
        weekday
    )
    SELECT 
        DISTINCT start_time, 
        EXTRACT(hour from start_time), 
        EXTRACT(day from start_time), 
        EXTRACT(week from start_time), 
        EXTRACT(month from start_time), 
        EXTRACT(year from start_time), 
        EXTRACT(weekday from start_time)
    FROM songplays
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
