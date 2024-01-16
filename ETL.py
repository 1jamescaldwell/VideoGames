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

gameTable1 = [] # Data from Video_Game.csv
gameTable2 = [] # Data from PopularGames.csv
gameTable3 = [] # Data from Games.csv

gameTable1.extend(videoGames1[['Name', 'Platform', 'Publisher', 'Genre']].values.tolist())
gameTable1 = [row + [None] for row in gameTable1] # Add one column of None's to the end of the list to match SQL table

gameTable2.extend(videoGames2[['Title', 'Genres', 'Summary']].values.tolist())
# Add None to the 2nd and 3rd columns, and shift 2/3 to 4/5
# Found a better way to do this for the other tables that I will use going forward. Keeping here in case this method comes in handy later
for row in gameTable2:
    row.insert(1, None)  # New 2nd column of None
    row.insert(2, None)  # New 3rd column of None
    row.insert(1, row.pop(1))  # Move the element from the new 2nd column to the 4th
    row.insert(2, row.pop(1))  # Move the element from the new 3rd column to the 5th

gameTable3.extend(videoGames3[['GameName', 'Console']].values.tolist())
gameTable3 = [row + [None] * 3 for row in gameTable3] # Add three rows of None to the end of each list

gameTable = gameTable1 + gameTable2 + gameTable3

# Replace nan's with None
gameTable = [[None if pd.isna(entry) else entry for entry in row] for row in gameTable]

#### VideoGameReviews Table
# gameReviews SQL Table is:
    # title, criticScore, userScore, userScoreCount, contentRating, userReview

gameReview1 = []
gameReview2 = []
gameReview3 = []

videoGames1R = videoGames1
videoGames1R['Critic_Score'] = videoGames1R['Critic_Score']/10 # Converting ratings to be out of 10
gameReview1.extend(videoGames1R[['Name', 'Critic_Score', 'User_Score', 'Critic_Count', 'User_Count','Rating']].values.tolist())
gameReview1 = [row + [None] for row in gameReview1] # Add one column of None's to the end of the list

videoGames2R = videoGames2
videoGames2R['Rating'] = videoGames2R['Rating']*2 # Converting ratings to be out of 10
gameReview2.extend(videoGames2R[['Title', 'Rating', 'Number of Reviews','Reviews']].values.tolist())
# Convert to Title, none, Rating, none, number of reviews, none, reviews to match SQL Table setup
gameReview2 = [[item, None, item2, None, item3, None, item4] for item, item2, item3, item4 in gameReview2]
# Replace string values (ex:'3.9k') with floats (3900) 
gameReview2 = [
    [
        (int(float(entry[:-1]) * 1000) if idx == 4 and isinstance(entry, str) and 'K' in entry else entry)
        for idx, entry in enumerate(row)
    ]
    for row in gameReview2
]

gameReview3.extend(videoGames3[['GameName', 'Score']].values.tolist())
# Add 5 columns of None to the end to match SQL table setup
gameReview3 = [row + [None] * 5 for row in gameReview3]

gameReview = gameReview1 + gameReview2 + gameReview3

# Replace nan's and tbd's with None
gameReview = [[None if (pd.isna(entry) or entry == 'tbd') else entry for entry in row] for row in gameReview]

#### Game Popularity Table
# game popularity SQL Table is:
    # title, userCount, plays, backlogs, wishlistCount
gamePopularity1 = []
gamePopularity2 = []

gamePopularity1.extend(videoGames1[['Name','User_Count']].values.tolist())
gamePopularity1 = [row + [None] * 3 for row in gamePopularity1] # Add 3 column of None's to the end of the list

gamePopularity2.extend(videoGames2[['Title','Plays','Backlogs','Wishlist']].values.tolist())
gamePopularity2 = [[item, None, item2, item3, item4] for item, item2, item3, item4 in gamePopularity2]

# Replace string values (ex:'3.9k') with floats (3900) 
gamePopularity2 = [
    [
        (int(float(entry[:-1]) * 1000) if idx > 1 and isinstance(entry, str) and 'K' in entry else entry)
        for idx, entry in enumerate(row)
    ]
    for row in gamePopularity2
]

gamePopularity = gamePopularity2 + gamePopularity2
# Replace nan's with None
gamePopularity = [[None if (pd.isna(entry)) else entry for entry in row] for row in gamePopularity]

#### Game Sales
# Game sales SQL Table is:
    # title, releaseYear, globalSales, publisher 
gameSales = []

gameSales.extend(videoGames1[['Name','Year_of_Release','Global_Sales','Publisher']].values.tolist()) #Sales are in millions
# Replace nan's with None
gameSales = [[None if (pd.isna(entry)) else entry for entry in row] for row in gameSales]

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


    # Calculate column widths and create the (?, ?, ?, ?) format that SQL insert command requires
    columnCount = len(records[0])
    tableFormat = '(?' + ', ?' * (columnCount-1) + ')'

    insert_statement = f"""
        INSERT INTO {tableName}
        VALUES {tableFormat}
    """

    try:
        for record in records:
            print(record) #Prints each line that's entered into SQL Sever
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


# tableName = 'VideoGamesInfo'
# SQL_SeverAdd(tableName,tableFormat,gameTable)
            
tableName = 'VideoGameReviews'
# SQL_SeverAdd(tableName,tableFormat,gameReview)

tableName = 'VideoGamePopularity'
# SQL_SeverAdd(tableName,tableFormat,gamePopularity)

tableName = 'VideoGameSales'
SQL_SeverAdd(tableName,gameSales)