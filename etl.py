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
