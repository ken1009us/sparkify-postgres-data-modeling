import os
import glob
import psycopg2
import pandas as pd
import json

from sql_queries import *


conn = psycopg2.connect("host=XXXXXX port=XXXX dbname=sparkifydb user=student password=student")


def get_files(data_dir):
    """
    Walk through the given directory, collect all JSON files, and load their contents into a DataFrame.

    Parameters:
        data_dir (str): The directory path that contains JSON files to be read.

    Returns:
        pd.DataFrame: A DataFrame containing all data aggregated from the JSON files found in the provided directory.
    """


    all_data = []
    for root, dirs, files in os.walk(data_dir):
        files = [f for f in files if f.endswith('.json')]
        for file in files:
            file_path = os.path.join(root, file)
            with open(file_path, 'r') as f:
                for line in f:
                    all_data.append(json.loads(line))

    if all_data:
        return pd.DataFrame(all_data)

    return pd.DataFrame()


def extract_song_data(songs_df):
    """
    Extract song data from the DataFrame and insert it into the 'songs' table in the database.

    Parameters:
        songs_df (pd.DataFrame): DataFrame containing song data.

    Side Effects:
        Inserts data into the 'songs' table of the database. Commits the transaction and handles any exceptions by rolling back.
    """

    selected_columns = ['song_id', 'title', 'artist_id', 'year', 'duration']
    song_df = songs_df[selected_columns]

    if not song_df.empty:
        song_data = song_df.values.tolist()
    else:
        song_data = []

    song_table_insert = ("INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES(%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING")

    try:
        cur = conn.cursor()
        for song in song_data:
            cur.execute(song_table_insert, song)
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"Error: {e}")
        if conn:
            conn.rollback()


def extract_artist_data(songs_df):
    """
    Extract artist data from the DataFrame and insert it into the 'artists' table in the database.

    Parameters:
        songs_df (pd.DataFrame): DataFrame containing artist data.

    Side Effects:
        Inserts data into the 'artists' table of the database. Commits the transaction and handles any exceptions by rolling back.
    """

    selected_columns = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    artist_df = songs_df[selected_columns]

    if not artist_df.empty:
        artist_data = artist_df.values.tolist()
    else:
        artist_data = []

    artist_table_insert = ("INSERT INTO artists (artist_id, name, location, latitude, longitude) VALUES(%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING")

    try:
        cur = conn.cursor()
        for artist in artist_data:
            cur.execute(artist_table_insert, artist)
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"Error: {e}")
        if conn:
            conn.rollback()


def extract_time_data(log_files_df):
    """
    Process log files to extract time data and insert it into the 'time' table in the database.

    Parameters:
        log_files_df (pd.DataFrame): DataFrame containing log entries.

    Side Effects:
        Inserts data into the 'time' table of the database. Commits the transaction and handles any exceptions by rolling back.
    """

    log_df = log_files_df[log_files_df['page'] == 'NextSong']
    log_df['ts'] = pd.to_datetime(log_df['ts'], unit='ms')

    time_data = {
        "start_time": log_df['ts'],
        "hour": log_df['ts'].dt.hour,
        "day": log_df['ts'].dt.day,
        "week": log_df['ts'].dt.isocalendar().week,
        "month": log_df['ts'].dt.month,
        "year": log_df['ts'].dt.year,
        "weekday": log_df['ts'].dt.weekday
    }

    column_labels = ["start_time", "hour", "day", "week", "month", "year", "weekday"]
    time_df = pd.DataFrame(time_data, columns=column_labels)

    if not time_df.empty:
        time_data = time_df.values.tolist()
    else:
        time_data = []

    time_table_insert = ("INSERT INTO time (start_time, hour, day, week, month, year, weekday) VALUES(%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING")

    try:
        cur = conn.cursor()
        for time in time_data:
            cur.execute(time_table_insert, time)
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"Error: {e}")
        if conn:
            conn.rollback()


def extract_user_data(log_files_df):
    """
    Extract user data from the log files DataFrame and insert/update it into the 'users' table in the database.

    Parameters:
        log_files_df (pd.DataFrame): DataFrame containing user data from log files.

    Side Effects:
        Inserts or updates data into the 'users' table of the database based on the user_id. Commits the transaction and handles any exceptions by rolling back.
    """

    user_df = log_files_df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    user_df = user_df[user_df['userId'].apply(lambda x: x.isdigit())]
    user_df = user_df.drop_duplicates(subset=['userId'])

    if not user_df.empty:
        user_data = user_df.values.tolist()
    else:
        user_data = []

    user_table_insert = ("INSERT INTO users (user_id, first_name, last_name, gender, level) VALUES(%s, %s, %s, %s, %s) \
                            ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level")

    try:
        cur = conn.cursor()
        for user in user_data:
            cur.execute(user_table_insert, user)
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"Error: {e}")
        if conn:
            conn.rollback()


def extract_songplay_data(log_files_df):
    """
    Process log files to extract songplay data and insert it into the 'songplays' table in the database.

    Parameters:
        log_files_df (pd.DataFrame): DataFrame containing log entries filtered by 'NextSong' action.

    Side Effects:
        Queries for matching song and artist IDs based on song title, artist name, and duration. Inserts songplay records into the 'songplays' table. Commits the transaction and handles any exceptions by rolling back.
    """

    song_select = ("SELECT songs.song_id, artists.artist_id FROM songs \
                    JOIN artists ON songs.artist_id = artists.artist_id \
                    WHERE songs.title = %s AND artists.name = %s AND songs.duration = %s")

    songplay_table_insert = ("INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) \
                                VALUES(%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING")

    filtered_df = log_files_df[(log_files_df['page'] == 'NextSong') & (log_files_df['userId'].apply(lambda x: x.isdigit()))]
    filtered_df['start_time'] = pd.to_datetime(filtered_df['ts'], unit='ms')
    log_files_list = filtered_df.to_dict('records')

    try:
        cur = conn.cursor()

        for row in log_files_list:
            cur.execute(song_select, (row['song'], row['artist'], row['length']))
            result = cur.fetchone()

            if result:
                song_id, artist_id = result
            else:
                song_id, artist_id = None, None

            songplay_data = (
                row['start_time'], row['userId'], row['level'], song_id, artist_id,
                row['sessionId'], row['location'], row['userAgent']
            )

            cur.execute(songplay_table_insert, songplay_data)

        conn.commit()
        cur.close()
    except Exception as e:
        print(f"Error: {e}")
        if conn:
            conn.rollback()


def main():
    """
    Main function to manage the workflow of loading data from files and inserting it into a PostgreSQL database.
    """

    # data_dir = "data/song_data"
    # songs_df = get_files(data_dir)

    # extract_song_data(songs_df)

    # extract_artist_data(songs_df)

    # log_data_dir = "data/log_data"
    # log_files_df = get_files(log_data_dir)

    # extract_time_data(log_files_df)

    # extract_user_data(log_files_df)

    # extract_songplay_data(log_files_df)

    conn.close()




if __name__ == "__main__":
    main()
