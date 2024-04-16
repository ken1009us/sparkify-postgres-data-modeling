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
