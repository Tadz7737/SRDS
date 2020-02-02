CREATE KEYSPACE IF NOT EXISTS Hotel
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

USE Hotel;

CREATE TABLE Rooms (
  roomId  int,
  startDate date,
  endDate date,
  name varchar,
  size int,
  PRIMARY KEY (roomId, startDate)
);

-- CREATE INDEX roomIndex ON Rooms (roomId);

-- CREATE TABLE MeteoData (
--   stationName varchar, 
--   measurement timestamp, 
--   temperature double,
--   pressure double,
--   PRIMARY KEY (stationName, measurement)
-- );

-- CREATE TABLE MeteoData2 (
--   stationName varchar, 
--   mDay varchar,
--   measurement timestamp, 
--   temperature double,
--   pressure double,
--   PRIMARY KEY ((stationName, mDay), measurement)
-- );

-- CREATE TABLE PageViewCounts (
--   url varchar,
--   views counter,
--   PRIMARY KEY(url)
-- );
