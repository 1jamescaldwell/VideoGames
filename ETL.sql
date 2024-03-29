-- These are all the SQL lines that I executed while creating my ETL project

-- Dropping a table
DROP TABLE Table_1;

-- Get Sever Name
SELECT @@SERVERNAME

-- Creating a table
CREATE TABLE VideoGames (
	gameIndex INT NOT NULL,
)

GO

SELECT * FROM VideoGames
GO

INSERT INTO VideoGames
VALUES 
	(1),
	(2)
GO

SELECT * FROM VideoGames
GO

-- Clear table
DELETE FROM VideoGames;

-- Add another column
ALTER TABLE dbo.VideoGames
ADD Platform VARCHAR(30) NULL;

-- After adding some data from python, check that it worked
SELECT TOP (1000) [gameIndex],[Platform]
  FROM [videoGames].[dbo].[VideoGames]

-- Select where videoIndex is < 100
SELECT * FROM VideoGames
WHERE gameIndex < 100;


-- Drop VideoGamesInfo
DROP TABLE VideoGamesInfo;

-- Creating GameInfo
CREATE TABLE VideoGamesInfo (
	title VARCHAR(255),
	console VARCHAR(100) NULL,
	publisher VARCHAR(255) NULL,
	genre VARCHAR(255) NULL,
	summary VARCHAR(5000) NULL,
)

-- Clear VideoGamesInfo
DELETE FROM VideoGamesInfo;

-- Select top 1000 from VideoGamesInfo table
SELECT TOP (1000) [title]
      ,[console]
      ,[publisher]
      ,[genre]
      ,[summary]
  FROM [videoGames].[dbo].[VideoGamesInfo]

-- Select 2nd 1000 and order by Title
SELECT [title]
      ,[console]
      ,[publisher]
      ,[genre]
      ,[summary]
FROM [videoGames].[dbo].[VideoGamesInfo]
ORDER BY [Title] -- Replace [YourOrderByColumn] with the column you want to order by
OFFSET 1000 ROWS
FETCH NEXT 1000 ROWS ONLY;

-- Drop VideoGamesReviews
DROP TABLE VideoGameReviews;

-- Creating GamReviews
CREATE TABLE VideoGameReviews (
	title VARCHAR(255),
	criticScore REAL NULL,
	userScore REAL NULL,
	criticScoreCount INT NULL,
	userScoreCount INT NULL,
	contentRating VARCHAR(50) NULL,
	-- userReview VARCHAR(8000) NULL, --Truly needed almost all 8000 characters for some of the reviews
	userReview TEXT NULL,
)

-- Select top 1000 from VideoGameReviews
SELECT TOP (1000) [title]
      ,[criticScore]
      ,[userScore]
      ,[criticScoreCount]
      ,[userScoreCount]
      ,[contentRating]
      ,[userReview]
  FROM [videoGames].[dbo].[VideoGameReviews]

-- Creating Game Popularity table
CREATE TABLE VideoGamePopularity (
	title VARCHAR(255),
	userCount INT NULL,
	plays INT NULL,
	backlogs INT NULL,
	wishlist INT NULL,
)

-- Top 1000 entreis from VideoGamePopularity
SELECT TOP (1000) [title]
      ,[userCount]
      ,[plays]
      ,[backlogs]
      ,[wishlist]
  FROM [videoGames].[dbo].[VideoGamePopularity]

-- Create Video Game Sales
CREATE TABLE VideoGameSales (
	title VARCHAR(255),
	releaseYear INT NULL,
	globalSales REAL NULL,
	publisher VARCHAR(255) NULL,
)

-- Select top 1000 from videoGameSales
SELECT TOP (1000) [title]
      ,[releaseYear]
      ,[globalSales]
      ,[publisher]
  FROM [videoGames].[dbo].[VideoGameSales]
