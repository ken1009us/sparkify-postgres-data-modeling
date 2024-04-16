import os
import glob
import psycopg2
import pandas as pd
import json

from sql_queries import *


conn = psycopg2.connect("host=127.0.0.1 port=5435 dbname=sparkifydb user=student password=student")


def get_files(data_dir):
    """Get all files in the data directory.

    Returns:
        list: A list of all files in the data directory
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

    print(time_df.head())


def main():
    # data_dir = "data/song_data"
    # songs_df = get_files(data_dir)
    # extract_song_data(songs_df)
    # extract_artist_data(songs_df)
    log_data_dir = "data/log_data"
    log_files_df = get_files(log_data_dir)
    extract_time_data(log_files_df)



if __name__ == "__main__":
    main()
