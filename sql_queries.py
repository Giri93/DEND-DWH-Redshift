import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_ENDPOINT = config.get("CLUSTER","DWH_ENDPOINT")
DB_NAME = config.get("CLUSTER","DB_NAME")
DB_USER = config.get("CLUSTER","DB_USER")
DB_PASSWORD = config.get("CLUSTER","DB_PASSWORD")
DB_PORT = config.get("CLUSTER","DB_PORT")

ARN = config.get("IAM_ROLE","ARN")

LOG_DATA = config.get("S3","LOG_DATA")
LOG_JSONPATH = config.get("S3","LOG_JSONPATH")
SONG_DATA = config.get("S3","SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users cascade"
song_table_drop = "DROP TABLE IF EXISTS songs cascade"
artist_table_drop = "DROP TABLE IF EXISTS artists cascade"
time_table_drop = "DROP TABLE IF EXISTS time cascade"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE staging_events(
artist VARCHAR,
auth VARCHAR,
firstName VARCHAR,
gender VARCHAR,
itemInSession SMALLINT,
lastName VARCHAR,
length DOUBLE PRECISION,
level VARCHAR,
location VARCHAR,
method VARCHAR,
page VARCHAR,
registration BIGINT,
sessionId SMALLINT,
song VARCHAR,
status SMALLINT,
ts BIGINT,
userAgent VARCHAR,
userId SMALLINT)
""")

staging_songs_table_create = ("""CREATE TABLE staging_songs(
num_songs SMALLINT,
artist_id varchar,
artist_latitude DOUBLE PRECISION,
artist_longitude DOUBLE PRECISION,
artist_location VARCHAR,
artist_name VARCHAR,
song_id varchar,
title varchar ,
duration DOUBLE PRECISION,
year SMALLINT)
""")

user_table_create = ("""CREATE TABLE users(
user_id SMALLINT NOT NULL PRIMARY KEY, 
first_name varchar NOT NULL, 
last_name varchar NOT NULL, 
gender varchar NOT NULL, 
level varchar NOT NULL)
diststyle all
""")

song_table_create = ("""CREATE TABLE songs(
song_id varchar NOT NULL PRIMARY KEY distkey, 
title varchar NOT NULL, 
artist_id varchar NOT NULL, 
year SMALLINT NOT NULL, 
duration DOUBLE PRECISION NOT NULL)
""")

artist_table_create = ("""CREATE TABLE artists(
artist_id varchar NOT NULL PRIMARY KEY, 
name varchar NOT NULL, 
location varchar NOT NULL, 
latitude varchar, 
longitude varchar)
""")

time_table_create = ("""CREATE TABLE time(
start_time TIMESTAMP NOT NULL PRIMARY KEY sortkey, 
hour SMALLINT NOT NULL, 
day SMALLINT NOT NULL, 
week SMALLINT NOT NULL, 
month SMALLINT NOT NULL, 
year SMALLINT NOT NULL, 
weekday varchar NOT NULL)
diststyle all
""")

songplay_table_create = ("""CREATE TABLE songplays(
songplay_id INT IDENTITY(0,1) NOT NULL PRIMARY KEY,
start_time TIMESTAMP NOT NULL, 
user_id SMALLINT NOT NULL, 
level varchar NOT NULL, 
song_id varchar NOT NULL distkey, 
artist_id varchar NOT NULL, 
session_id SMALLINT NOT NULL, 
location varchar NOT NULL, 
user_agent varchar NOT NULL,
FOREIGN KEY(start_time) REFERENCES time(start_time),
FOREIGN KEY(user_id) REFERENCES users(user_id),
FOREIGN KEY(song_id) REFERENCES songs(song_id),
FOREIGN KEY(artist_id) REFERENCES artists(artist_id))
""")

# STAGING TABLES

staging_events_copy = """copy staging_events from '{}'
        credentials 'aws_iam_role={}'
        region 'us-west-2'
        format as JSON '{}';
""".format(LOG_DATA,ARN,LOG_JSONPATH)

staging_songs_copy = """copy staging_songs from '{}'
        credentials 'aws_iam_role={}'
        region 'us-west-2'
        json 'auto';
""".format(SONG_DATA,ARN)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT timestamp 'epoch' + stg_evnt.ts/1000 * interval '1 second', stg_evnt.userId, stg_evnt.level, stg_sngs.song_id, stg_sngs.artist_id, stg_evnt.sessionId, stg_evnt.location, stg_evnt.userAgent
FROM staging_events stg_evnt
INNER JOIN staging_songs stg_sngs
ON stg_sngs.title = stg_evnt.song
AND stg_sngs.artist_name = stg_evnt.artist
AND stg_sngs.duration = stg_evnt.length
WHERE stg_evnt.page = 'NextSong';
""")

user_table_insert = ("""INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT userId,firstName,lastName,gender,level
FROM (
  SELECT userId,firstName,lastName,gender,level, 
  row_number() over(partition by userId order by ts desc) rn 
  FROM staging_events 
  WHERE page='NextSong'
) stg_evnts
WHERE rn=1;
""")

song_table_insert = ("""INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT song_id, title, artist_id, year, duration
FROM staging_songs;
""")

artist_table_insert = ("""INSERT INTO artists(artist_id, name, location, latitude, longitude)
SELECT artist_id,artist_name,COALESCE(artist_location, 'not known'),artist_latitude,artist_longitude
FROM staging_songs;
""")

time_table_insert = ("""INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT start_time, EXTRACT(hour from start_time), EXTRACT(day from start_time), EXTRACT(week from start_time), EXTRACT(month from start_time), EXTRACT(year from start_time), EXTRACT(dow from start_time) FROM 
(SELECT  timestamp 'epoch' + ts/1000 * interval '1 second' AS start_time FROM staging_events 
WHERE page='NextSong') t1
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
