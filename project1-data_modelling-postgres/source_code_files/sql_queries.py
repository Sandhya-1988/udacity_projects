# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays_table"
user_table_drop     = "DROP TABLE IF EXISTS user_table"
song_table_drop     = "DROP TABLE IF EXISTS song_table"
artist_table_drop   = "DROP TABLE IF EXISTS artist_table"
time_table_drop     = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays_table (
        songplayId SERIAL NOT NULL,
        start_time timestamp NOT NULL,
        userId varchar NOT NULL,
        level varchar NOT NULL,
        song_id varchar,
        artist_id varchar,
        sessionId int NOT NULL,
        location varchar,
        userAgent varchar NOT NULL,
        PRIMARY KEY(songplayId)
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS user_table (
        userId varchar PRIMARY KEY NOT NULL,
        firstName varchar NOT NULL,
        lastName varchar NOT NULL,
        gender char NOT NULL,
        level varchar NOT NULL
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS song_table (
        song_id varchar NOT NULL,
        title varchar NOT NULL,
        artist_id varchar NOT NULL,
        year int,
        duration numeric,
        PRIMARY KEY(song_id)
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist_table (
        artist_id varchar NOT NULL,
        artist_name varchar NOT NULL,
        artist_location varchar,
        artist_latitude numeric,
        artist_longitude numeric,
        PRIMARY KEY(artist_id)
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time_table (
        timetableId SERIAL NOT NULL,
        timestamp timestamp NOT NULL,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int,
        PRIMARY KEY(timetableId)
    );
""")

# INSERT RECORDS

songplay_table_insert = ("""
    INSERT INTO songplays_table (
        start_time,userId,
        level,song_id,artist_id,
        sessionId,location,userAgent)
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (songplayId) DO NOTHING
""")

user_table_insert = ("""
    INSERT INTO user_table (
        userId,firstname,
        lastname,gender,level) 
    VALUES(%s,%s,%s,%s,%s)
    ON CONFLICT (userId) DO UPDATE SET 
    level = EXCLUDED.level
""")

song_table_insert = ("""
    INSERT INTO song_table (
        artist_id,song_id,
        duration,year,title)
    VALUES(%s,%s,%s,%s,%s) 
    ON CONFLICT (song_id) DO NOTHING 
""")

artist_table_insert = ("""
    INSERT INTO artist_table (
        artist_id,artist_name,
        artist_location,artist_latitude,
        artist_longitude)
    VALUES(%s,%s,%s,%s,%s) 
    ON CONFLICT (artist_id) DO NOTHING
""")

time_table_insert = ("""
    INSERT INTO time_table (
        timestamp,hour,
        day,week,month,
        year,weekday) 
    VALUES(%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT DO NOTHING 
""")

# FIND SONGS

song_select = ("""
    SELECT song_table.song_id,artist_table.artist_id 
    FROM song_table JOIN artist_table 
    ON(song_table.artist_id = artist_table.artist_id) 
    WHERE song_table.title=%s AND 
    artist_table.artist_name=%s AND song_table.duration=%s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries   = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]