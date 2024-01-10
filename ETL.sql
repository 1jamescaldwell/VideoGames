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


