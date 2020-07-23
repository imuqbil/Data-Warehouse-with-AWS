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

staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS staging_events (

event_id       INTEGER IDENTITY(0,1),
artist         VARCHAR ,
auth           VARCHAR ,
firstName      VARCHAR ,
gender         VARCHAR ,
itemInSession  INTEGER,
lastName     VARCHAR ,
length       FLOAT ,
level        VARCHAR ,
location     VARCHAR ,
method       VARCHAR , 
page         VARCHAR,
registration FLOAT,
sessionId    INTEGER  NOT NULL,
song         VARCHAR , 
status       INTEGER , 
ts           TIMESTAMP NOT NULL,
userAgent    VARCHAR , 
userId       INTEGER  ); """)

staging_songs_table_create = ("""  CREATE TABLE IF NOT EXISTS staging_songs (
    
num_songs         INTEGER,
artist_id         VARCHAR  NOT NULL,
artist_latitude   FLOAT,
artist_longitude  FLOAT,
artist_location   VARCHAR,
artist_name       VARCHAR,
song_id           VARCHAR NOT NULL,
title             VARCHAR,
duration          FLOAT,
year              INTEGER ); """)


songplay_table_create = ( """ CREATE TABLE IF NOT EXISTS songplays (songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,  
                                                                  start_time timestamp NOT NULL , 
                                                                  user_id INTEGER NOT NULL REFERENCES users(user_id) , 
                                                                  level varchar, 
                                                                  song_id varchar NOT NULL REFERENCES songs(song_id) , 
                                                                  artist_id varchar NOT NULL REFERENCES artists(artist_id), 
                                                                  session_id int,
                                                                  location varchar,
                                                                  user_agent varchar); """ )


user_table_create = ("""CREATE TABLE IF NOT EXISTS users (      user_id INTEGER NOT NULL PRIMARY KEY,  
                                                                first_name varchar,
                                                                last_name  varchar, 
                                                                gender     varchar, 
                                                                level      varchar ); """)

song_table_create = (""" CREATE TABLE IF NOT EXISTS songs (     song_id text  NOT NULL PRIMARY KEY,  
                                                                title text,
                                                                artist_id text NOT NULL, 
                                                                year int, 
                                                                duration FLOAT ); """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (  artist_id  varchar NOT NULL PRIMARY KEY, 
                                                                name       varchar, 
                                                                location   varchar,
                                                                latitude   FLOAT, 
                                                                longitude  FLOAT );""" )

time_table_create = (""" CREATE TABLE IF NOT EXISTS time (  start_time timestamp NOT NULL PRIMARY KEY,
                                                            hour    SMALLINT, 
                                                            day     SMALLINT,
                                                            week    SMALLINT, 
                                                            month   SMALLINT,
                                                            year    SMALLINT,
                                                            weekday SMALLINT ); """)

# STAGING TABLES

staging_events_copy = (""" COPY staging_events 
                          FROM {} 
                          iam_role {}
                          JSON {} 
                          STATUPDATE TRUE 
                          TIMEFORMAT as 'epochmillisecs'; """).format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])
                                                                      

staging_songs_copy = (""" COPY staging_songs 
                         FROM {}
                         iam_role {}
                         format as JSON 'auto' 
                         ACCEPTINVCHARS AS '^' 
                         STATUPDATE TRUE;  """).format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = (""" INSERT INTO songplays (start_time, user_id, level,
                             song_id, artist_id, session_id, location, user_agent) 
                             SELECT  staging_events.ts                   AS start_time, 
                                     staging_events.userId               AS user_id,
                                     staging_events.level                AS level,
                                     staging_songs.song_id               AS song_id,
                                     staging_songs.artist_id             AS artist_id,
                                     staging_events.sessionId            AS session_id,
                                     staging_events.location             AS location,
                                     staging_events.userAgent            AS user_agent
                            FROM  staging_events
                            JOIN  staging_songs ON (( staging_events.artist = staging_songs.artist_name
                                                       AND staging_events.song = staging_songs.title) )
                            WHERE staging_events.page='NextSong';  """) 


user_table_insert = (""" INSERT INTO users (user_id, first_name, last_name, gender, level) 
                         SELECT  DISTINCT  staging_events.userId     AS user_id,
                                           staging_events.firstName  AS first_name,
                                           staging_events.lastName   AS last_name,
                                           staging_events.gender,
                                           staging_events.level      
                         FROM staging_events 
                         WHERE userId IS NOT NULL;  """)
                         
                         
song_table_insert = (""" INSERT INTO songs (song_id, title, artist_id, year, duration)
                     SELECT  DISTINCT       staging_songs.song_id, 
                                            staging_songs.title,
                                            staging_songs.artist_id, 
                                            staging_songs.year, 
                                            staging_songs.duration
                     FROM staging_songs;  """)
                                          

artist_table_insert = (""" INSERT INTO artists (artist_id, name, location, latitude, longitude)
                       SELECT DISTINCT    staging_songs.artist_id, 
                                          staging_songs.artist_name     AS name, 
                                          staging_songs.artist_location AS location,
                                          staging_songs.artist_latitude AS latitude, 
                                          staging_songs.artist_longitude AS longitude
                       FROM staging_songs; """)
                       

time_table_insert = (""" INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                     SELECT  start_time, 
                     EXTRACT(hour FROM start_time) as hour,  
                     EXTRACT(day FROM start_time) as day,
                     EXTRACT(week FROM start_time) as week,
                     EXTRACT(month FROM start_time) as month,
                     EXTRACT(year FROM start_time) as year,
                     EXTRACT(weekday FROM start_time) as weekday
                     FROM "songplays"; """)
  
# QUERY LISTS

create_table_queries = [ user_table_create, song_table_create, artist_table_create, time_table_create,songplay_table_create, staging_events_table_create, staging_songs_table_create]
drop_table_queries = [ songplay_table_drop,user_table_drop, song_table_drop, artist_table_drop, time_table_drop ,staging_events_table_drop,staging_songs_table_drop]
copy_table_queries = [ staging_events_copy, staging_songs_copy]
insert_table_queries = [ user_table_insert, song_table_insert, artist_table_insert,songplay_table_insert, time_table_insert]
