import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
IAM_ROLE = config.get('IAM_ROLE', 'ARN')
LOGS_JSON_DATA = config.get('S3', 'LOG_DATA')
LOGS_JSON_PATH = config.get('S3', 'LOG_JSONPATH')
SONGS_JSON_DATA = config.get('S3', 'SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP table IF EXISTS staging_events"
staging_songs_table_drop = "DROP table IF EXISTS staging_songs"
songplay_table_drop = "DROP table IF EXISTS songplays"
user_table_drop = "DROP table IF EXISTS users"
song_table_drop = "DROP table IF EXISTS songs"
artist_table_drop = "DROP table IF EXISTS artists"
time_table_drop = "DROP table IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS staging_events (
                            artist VARCHAR,
                            auth VARCHAR,
                            first_name VARCHAR,
                            gender VARCHAR,
                            item_in_session INTEGER NOT NULL,
                            last_name VARCHAR,
                            length DECIMAL,
                            level VARCHAR,
                            location VARCHAR,
                            method VARCHAR ,
                            page VARCHAR,
                            registration DECIMAL,
                            session_id INTEGER NOT NULL,
                            song VARCHAR,
                            status VARCHAR,
                            ts BIGINT,
                            user_agent VARCHAR,
                            user_id INTEGER
                        );""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
                        num_songs INTEGER NOT NULL,
						artist_id VARCHAR DISTKEY,
						artist_latitude DOUBLE precision,
						artist_longitude DOUBLE precision,
						artist_location VARCHAR(2048),
						artist_name VARCHAR(2048),
						song_id VARCHAR,
						title VARCHAR(2048),
						duration DECIMAL,
						year INTEGER
                        )diststyle key;""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
                        songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY, 
                        start_time TIMESTAMP NOT NULL SORTKEY, 
                        user_id INTEGER NOT NULL DISTKEY, 
                        level VARCHAR, 
                        song_id VARCHAR, 
                        artist_id VARCHAR, 
                        session_id VARCHAR, 
                        location VARCHAR, 
                        user_agent VARCHAR)
                        diststyle key;""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
                     user_id INTEGER PRIMARY KEY DISTKEY SORTKEY, 
                     first_name  VARCHAR NOT NULL, 
                     last_name VARCHAR NOT NULL , 
                     gender VARCHAR NOT NULL , 
                     level VARCHAR) 
                     diststyle key;""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
                     song_id VARCHAR PRIMARY KEY SORTKEY, 
                     title VARCHAR(2048) NOT NULL, 
                     artist_id VARCHAR NOT NULL DISTKEY, 
                     year INTEGER, 
                     duration DECIMAL NOT NULL
                     ) 
                     diststyle key;""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(
                      artist_id VARCHAR PRIMARY KEY SORTKEY, 
                      name VARCHAR(2048) NOT NULL, 
                      location VARCHAR(2048), 
                      latitude DOUBLE precision, 
                      longitude DOUBLE precision) 
                      diststyle all;""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS  (
                     start_time TIMESTAMP PRIMARY KEY SORTKEY, 
                     hour INTEGER NOT NULL, 
                     day INTEGER NOT NULL, 
                     week INTEGER NOT NULL, 
                     month INTEGER NOT NULL, 
                     year INTEGER NOT NULL DISTKEY, 
                     weekday INTEGER NOT NULL) 
                     diststyle key;""")

# STAGING TABLES

staging_events_copy = ("""
    COPY {} FROM {}
    IAM_ROLE '{}'
    JSON {};
""").format(
    'staging_events',
    LOGS_JSON_DATA,
    IAM_ROLE,
    LOGS_JSON_PATH
)

staging_songs_copy = ("""
    COPY {} FROM {}
    IAM_ROLE '{}'
    JSON 'auto';
""").format(
    'staging_songs',
    SONGS_JSON_DATA,
    IAM_ROLE
)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) SELECT
                        TIMESTAMP 'epoch' + (e.ts/1000 * INTERVAL '1 second'),
                        e.user_id,
                        e.level,
                        s.song_id,
                        s.artist_id,
                        e.session_id,
                        e.location,
                        e.user_agent
                        FROM staging_events e
                        LEFT JOIN staging_songs s ON
                            e.song = s.title AND
                            e.artist = s.artist_name
                        WHERE
                            e.page = 'NextSong'
                       """)

user_table_insert = ("""INSERT INTO users SELECT DISTINCT (user_id)
                    user_id,
                    first_name,
                    last_name,
                    gender,
                    level
                    FROM staging_events
                    WHERE user_id is not null
                    """)

song_table_insert = ("""INSERT INTO songs SELECT DISTINCT (song_id)
                    song_id,
                    title,
                    artist_id,
                    year,
                    duration
                    FROM staging_songs
                    WHERE song_id is not null
                    """)

artist_table_insert = ("""INSERT INTO artists SELECT DISTINCT (artist_id)
                       artist_id,
                       artist_name,
                       artist_location,
                       artist_latitude,
                       artist_longitude
                       FROM staging_songs
                       WHERE artist_id is not null
                      """)

time_table_insert = ("""INSERT INTO time
                    with temp_timeStamp AS (select TIMESTAMP 'epoch' + (ts/1000 * INTERVAL '1 second') as ts FROM staging_events)
                    SELECT DISTINCT
                    ts,
                    extract(hour from ts),
                    extract(day from ts),
                    extract(week from ts),
                    extract(month from ts),
                    extract(year from ts),
                    extract(dow from ts)
                    FROM temp_timeStamp
                    """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert,
                        time_table_insert]
