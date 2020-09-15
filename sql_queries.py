import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
template_table_drop = "DROP TABLE IF EXISTS {}"
staging_events_table_drop = template_table_drop.format("staging_events")
staging_songs_table_drop = template_table_drop.format("staging_songs")
songplay_table_drop = template_table_drop.format("songplays")
user_table_drop = template_table_drop.format("users")
song_table_drop = template_table_drop.format("songs")
artist_table_drop = template_table_drop.format("artists")
time_table_drop = template_table_drop.format("time")

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
    artist VARCHAR,
    auth VARCHAR,
    first_name VARCHAR,
    gender VARCHAR,
    item_in_session INT,
    last_name VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    session_id INT,
    song VARCHAR,
    status INT,
    start_time BIGINT,
    user_agent VARCHAR,
    user_id VARCHAR
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
    song_id VARCHAR,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    title VARCHAR,
    duration FLOAT,
    year SMALLINT
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(
    songplay_id INT IDENTITY(0,1),
    start_time BIGINT NOT NULL sortkey,
    user_id INT NOT NULL REFERENCES users(user_id),
    level TEXT NOT NULL,
    song_id VARCHAR(25) NOT NULL REFERENCES songs(song_id) distkey,
    artist_id VARCHAR(25) NOT NULL REFERENCES artists(artist_id),
    session_id INT NOT NULL,
    location TEXT,
    user_agent TEXT,
    PRIMARY KEY(songplay_id)
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
    user_id int sortkey,
    first_name TEXT,
    last_name TEXT,
    gender VARCHAR(5),
    level TEXT,
    PRIMARY KEY(user_id)
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
    song_id VARCHAR(25) sortkey,
    title TEXT NOT NULL,
    artist_id VARCHAR(25) NOT NULL,
    year INT,
    duration FLOAT,
    PRIMARY KEY(song_id)
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
    artist_id VARCHAR(25) sortkey,
    artist_name TEXT NOT NULL,
    location TEXT,
    latitude FLOAT,
    longitude FLOAT,
    PRIMARY KEY(artist_id)
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
    start_time BIGINT sortkey,
    hour SMALLINT NOT NULL,
    day SMALLINT NOT NULL,
    week INT NOT NULL,
    month SMALLINT NOT NULL,
    year INT NOT NULL,
    weekday SMALLINT NOT NULL,
    PRIMARY KEY(start_time)
)
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
IAM_ROLE {}
REGION 'us-west-2'
FORMAT AS JSON {}
""").format(config['S3']['LOG_DATA'], 
           config['IAM_ROLE']['ARN'],
           config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs FROM {}
IAM_ROLE {}
REGION 'us-west-2'
FORMAT AS JSON 'auto'
""").format(config['S3']['SONG_DATA'],
           config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT start_time,
        user_id::integer,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
FROM staging_events e
JOIN staging_songs s ON (e.song = s.title and e.artist = s.artist_name) AND e.page = 'NextSong' 
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT user_id::integer,
                first_name,
                last_name,
                gender,
                level
FROM staging_events
WHERE page = 'NextSong' AND user_id NOT IN (SELECT DISTINCT user_id FROM users)
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id as song_id,
        title as title,
        artist_id as artist_id,
        year as year,
        duration as duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, artist_name, location, latitude, longitude)
SELECT DISTINCT s.artist_id as artist_id,
                s.artist_name as artist_name,
                s.artist_location as location,
                s.artist_latitude as latitude,
                s.artist_longitude as longitude
FROM staging_songs s
WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists)
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT  DISTINCT s.start_time as start_time,
                EXTRACT(hour from DATEADD(s, s.start_time / 1000, '1970-01-01 00:00:00')) as hour,
                EXTRACT(day from DATEADD(s, s.start_time / 1000, '1970-01-01 00:00:00')) as day,
                EXTRACT(week from DATEADD(s, s.start_time / 1000, '1970-01-01 00:00:00')) as week,
                EXTRACT(month from DATEADD(s, s.start_time / 1000, '1970-01-01 00:00:00')) as month,
                EXTRACT(year from DATEADD(s, s.start_time / 1000, '1970-01-01 00:00:00')) as year,
                EXTRACT(weekday from DATEADD(s, s.start_time / 1000, '1970-01-01 00:00:00')) as weekday
FROM songplays s
WHERE start_time NOT IN (SELECT DISTINCT start_time FROM time)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, songplay_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert,songplay_table_insert, time_table_insert]
