import os
import glob
import json
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Description: This function can be used to read song_data json file from filepath(data/song_data), which will be used later 
    to get the song and artist information and populate song_table and artist_table based on that.

    Arguments: 
    cur-> cursor object to execute queries
    filepath-> contains the filename with its complete path
    
    Returns:Nothing
    """
    # open song file
    with open (filepath) as f:
        file_dict = json.load(f)
    df = pd.DataFrame(file_dict, index=[0])

    # insert song record
    df1 = df[['artist_id','song_id','duration','year','title']]
    result=df1.values
    song_data=result.tolist() 
    cur.execute(song_table_insert, song_data[0])
    
    # insert artist record
    df2=df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']]
    result_artist=df2.values
    artist_data = result_artist.tolist()
    cur.execute(artist_table_insert, artist_data[0])


def process_log_file(cur, filepath):
    
    """
    Description: This function can be used to read log_data json file from filepath(data/log_data), which will be used later 
    to get the user,time and songplay information and populate user_table and time_table based on that.

    Arguments: 
    cur-> cursor object to execute queries
    filepath-> contains the filename with its complete path
    
    Returns:Nothing
    """
    
    # open log file
    df_list=[]
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df = df.loc[df['page'] == "NextSong"]
    df.head()

    # convert timestamp column to datetime
    df['ts']=pd.to_datetime(df['ts'] ,unit='ms')
    t = df['ts']
    t.head() 
    
    # insert time data records
    time_data = (t,t.dt.hour,t.dt.day,t.dt.week,t.dt.month,t.dt.year,t.dt.weekday)
    column_labels = ['timestamp','hour', 'day', 'week', 'month', 'year','weekday']

    time_dict= dict(zip(column_labels,time_data))
    time_df = pd.DataFrame.from_dict(time_dict)
    #time_df=data.join(pd.concat((getattr(data['timestamp'].dt, i).rename(i) for i in column_labels), axis=1)) 

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.drop_duplicates().iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent) 
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Description: This function is called from main function and is responsible to call function for extracting song_data data and     log_data and inserting them into their respective tables.

    Arguments: 
    cur-> cursor object to execute queries
    conn->Connectio to the database which was established in main function
    filepath-> contains the filename with its complete path
    func->Name of the function to be processed.First for song_data dn then for log_data.
    
    Returns:Nothing
    """
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    
    """
    Description: Main function to call other functions to process the data in data files and achieve the desired output.
    NO Arguments and Returns nothing.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()