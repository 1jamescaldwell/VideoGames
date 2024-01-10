# Video Game ETL Project
# James Caldwell January 2024
# Extract, Transform, Load Project
    # Various video game centered datasets in .CSV's
    # Extract: Data is loaded into python
    # Transform: data manipulation in python
    # Load: Put data into an SQL database 

import sys
import pypyodbc as odbc
import pandas as pd 
import os


videoGames1 = pd.read_csv('Video_Games.csv')
  # Video_Game.csv from https://www.kaggle.com/datasets/thedevastator/global-video-game-sales-and-reviews
  # Popular Games data from https://www.kaggle.com/datasets/arnabchaki/popular-video-games-1980-2023
  # Steam review data from https://www.kaggle.com/datasets/forgemaster/steam-reviews-dataset


# records = videoGames1.index.tolist()
records = videoGames1[['index', 'Platform']].values.tolist()

# records = [ 
#     [1,'Wii'],
#     [2,'PS2']
# ]

# Convert each element of the list to a list (each inner list represents a row)
# records = [[value] for value in records]

DRIVER = 'SQL Server'
SERVER_NAME = r'caldwell-lt\SQLEXPRESS'
DATABASE_NAME = 'VideoGames'

conn_string = f"""
    Driver={{{DRIVER}}};
    Server={SERVER_NAME};
    Database={DATABASE_NAME};
    Trust_Connection=yes;
"""

try:
    conn = odbc.connect(conn_string)
except Exception as e:
    print(e)
    print('task is terminated')
    sys.exit()
else:
    cursor = conn.cursor()


insert_statement = """
    INSERT INTO VideoGames
    VALUES (?,?)
"""

try:
    for record in records:
        print(record)
        cursor.execute(insert_statement, record)        
except Exception as e:
    cursor.rollback()
    print(e.value)
    print('transaction rolled back')
else:
    print('records inserted successfully')
    cursor.commit()
    cursor.close()
finally:
    if conn.connected == 1:
        print('connection closed')
        conn.close()
