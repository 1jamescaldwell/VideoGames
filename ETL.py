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
import numpy as np
import os

# Read the CSV files
  # Video_Game.csv from https://www.kaggle.com/datasets/thedevastator/global-video-game-sales-and-reviews
  # PopularGames data from https://www.kaggle.com/datasets/arnabchaki/popular-video-games-1980-2023
  # Games.csv data from https://www.kaggle.com/datasets/tamber/steam-video-games
videoGames1 = pd.read_csv('Video_Games.csv')
videoGames2 = pd.read_csv('PopularGames.csv')
videoGames3 = pd.read_csv('Games.csv')

# Print information about each dataframe
print("Information for videoGames1:")
print("Number of entries:", videoGames1.shape[0])
print("Number of columns:", videoGames1.shape[1])
print("Column names:", videoGames1.columns.tolist())

print("\nInformation for videoGames2:")
print("Number of entries:", videoGames2.shape[0])
print("Number of columns:", videoGames2.shape[1])
print("Column names:", videoGames2.columns.tolist())

print("\nInformation for videoGames3:")
print("Number of entries:", videoGames3.shape[0])
print("Number of columns:", videoGames3.shape[1])
print("Column names:", videoGames3.columns.tolist())

#### VideoGamesInfo Table
# gameInfo SQL Table is:
    # Title, console (platform), publisher (developer), genre, summary
    # There will be a merge of data from all 3 data sources for this table

gameTable1 = []
gameTable2 = []
gameTable3 = []

gameTable1.extend(videoGames1[['Name', 'Platform', 'Publisher', 'Genre']].values.tolist())
gameTable1 = [row + [None] for row in gameTable1]

gameTable2.extend(videoGames2[['Title', 'Genres', 'Summary']].values.tolist())
# Add None to the 2nd and 3rd columns, and shift 2/3 to 4/5
for row in gameTable2:
    row.insert(1, None)  # New 2nd column of None
    row.insert(2, None)  # New 3rd column of None
    row.insert(1, row.pop(1))  # Move the element from the new 2nd column to the 4th
    row.insert(2, row.pop(1))  # Move the element from the new 3rd column to the 5th

gameTable3.extend(videoGames3[['GameName', 'Console']].values.tolist())
gameTable3 = [row + [None] * 3 for row in gameTable3]

gameTable = gameTable1 + gameTable2 + gameTable3

# Replace nan's with None
gameTable = [[None if pd.isna(entry) else entry for entry in row] for row in gameTable]

# records = gameTable

def SQL_SeverAdd(tableName,records):
    print('\n\nAdding Data to SQL Server')
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
        print('Task is terminated')
        sys.exit()
    else:
        cursor = conn.cursor()


    insert_statement = f"""
        INSERT INTO {tableName}
        VALUES (?,?,?,?,?)
    """

    try:
        for record in records:
            # print(record) #Prints each line that's entered into SQL Sever
            cursor.execute(insert_statement, record)        
    except Exception as e:
        cursor.rollback()
        print(e.value)
        print('Transaction rolled back')
    else:
        print('Records inserted successfully')
        cursor.commit()
        cursor.close()
    finally:
        if conn.connected == 1:
            print('Connection closed')
            conn.close()

serverAdd = 1
if serverAdd == 1:
    tableName = 'VideoGamesInfo'
    SQL_SeverAdd(tableName,gameTable)
