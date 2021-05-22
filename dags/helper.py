import glob
import sqlite3
import pandas as pd

def read_table_names(db):
    conn = sqlite3.connect(db)
    with conn:
        tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type ='table' AND name NOT LIKE 'sqlite_%';", conn)
    return tables.name

def find_file_names(pattern):
    filenames = []
    for filename in glob.glob(pattern):
        filenames.append(filename)
    return filenames
