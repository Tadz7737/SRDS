CREATE KEYSPACE IF NOT EXISTS Hotel
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

USE Hotel;

CREATE TABLE Rooms (
  roomId  int,
  rDate date,
  name varchar,
  size int,
  PRIMARY KEY (rDate, roomId)
);
