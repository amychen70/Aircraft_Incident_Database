library(DBI)
library(RMySQL)

conn <- dbConnect(
  RMySQL::MySQL(),
  dbname = ,
  host = ,
  port = , 
  user = ,
  password = 
)

## Ensure everything is dropped before running 
status <- dbExecute(conn, "DROP PROCEDURE IF EXISTS storeIncident")
status <- dbExecute(conn, "DROP PROCEDURE IF EXISTS storeNewIncident")
status <- dbExecute(conn, "DELETE FROM Incident WHERE iid = 'test'")
status <- dbExecute(conn, "DELETE FROM Incident WHERE iid = 'test1'")

##------------------------------------------------------------------------------
## Mei Qi (Amy) Chen

sql <- "
CREATE PROCEDURE storeIncident (
IN iid VARCHAR(10),
IN date VARCHAR(20), 
IN incidentType VARCHAR(20),
IN severity VARCHAR(20),
IN delay INT,
IN num_injuries INT,
IN reported_by VARCHAR(20),
IN fid VARCHAR(5)
)
BEGIN
INSERT INTO Incident (
  iid,
  date,
  incident_type,
  severity,
  delay_mins,
  num_injuries,
  reported_by,
  fid
)
VALUES (
  iid,
  date, 
  incidentType,
  severity,
  delay,
  num_injuries,
  reported_by,
  fid
);
END;
"

## Create storeIncident Procedure
status <- dbExecute(conn, sql)


sql <- "
CALL storeIncident(
'test',
'01.1.2025',
'mechanical',
'minor',
45,
0,
'crew',
'10'
);
"

## Call Procedure, Insert 
status <- dbExecute(conn, sql)

## Check if added 
sql <- "SELECT * FROM Incident WHERE iid = 'test'"
rs <- dbGetQuery(conn, sql)
print(rs)

## Delete 
status <- dbExecute(conn, "DELETE FROM Incident WHERE iid = 'test'")

## Check after deleted 
sql <- "SELECT * FROM Incident WHERE iid = 'test'"
rs <- dbGetQuery(conn, sql)
print(rs)

##------------------------------------------------------------------------------
## David Li 

sql <- "
CREATE PROCEDURE storeNewIncident (
  IN iid VARCHAR(10),
  IN `date` VARCHAR(20), 
  IN incidentType VARCHAR(20),
  IN severity VARCHAR(20),
  IN delay INT,
  IN num_injuries INT,
  IN reported_by VARCHAR(20),
  
  IN fid VARCHAR(5),
  IN flight_number INT,
  IN airline_name VARCHAR(50),
  IN aircraft_name VARCHAR(50),
  IN airport_name VARCHAR(50)
)
BEGIN
  DECLARE airlineExists INT DEFAULT 0;
  DECLARE aircraftExists INT DEFAULT 0;
  DECLARE airportExists INT DEFAULT 0;
  DECLARE flightExists INT DEFAULT 0;


INSERT IGNORE INTO Airline (airline_name) VALUES (airline_name);
INSERT IGNORE INTO Aircraft (aircraft_name) VALUES (aircraft_name);
INSERT IGNORE INTO Dep_Airport (airport_name) VALUES (airport_name);
INSERT IGNORE INTO Flights (fid, flight_number, airline, aircraft, dep_airport)
VALUES (fid, flight_number, airline_name, aircraft_name, airport_name);


INSERT INTO Incident (
  iid,
  date,
  incident_type,
  severity,
  delay_mins,
  num_injuries,
  reported_by,
  fid
)
VALUES (
  iid,
  date, 
  incidentType,
  severity,
  delay,
  num_injuries,
  reported_by,
  fid
);
END;
"

## Create storeNewIncident Procedure 
status <- dbExecute(conn, sql)

sql <- "
CALL storeNewIncident(
'test1',
'01.1.2025',
'mechanical',
'minor',
45,
0,
'crew',
'10000',
'2000',
'PP',
'CHENS-BIRD',
'PPP'
);
"

## Call Procedure, Insert 
status <- dbExecute(conn, sql)

## Check after added 
sql <- "SELECT * FROM Flights WHERE flight_number = '2000'"
rs <- dbGetQuery(conn, sql);
print(rs)

## Check if airline PP exists
sql <- "SELECT * FROM Airline WHERE airline_name = 'PP'"
rs <- dbGetQuery(conn, sql);
print(rs)

## Checking if aircraft CHENS-BIRD exists
sql <- "SELECT * FROM Aircraft WHERE aircraft_name = 'CHENS-BIRD'"
rs <- dbGetQuery(conn, sql);
print(rs)

## Checking if airport PPP exists
sql <- "SELECT * FROM Dep_Airport WHERE airport_name = 'PPP'"
rs <- dbGetQuery(conn, sql);
print(rs)

## Check Incident Table 
sql <- "SELECT * FROM Incident WHERE iid = 'test1'"
rs <- dbGetQuery(conn, sql)
print(rs)

## Check Flights Table 
sql <- "SELECT * FROM Flights WHERE fid = '10000'"
rs <- dbGetQuery(conn, sql)
print(rs)

## Delete test1
status <- dbExecute(conn, "DELETE FROM Incident WHERE iid = 'test1'")
status <- dbExecute(conn, "DELETE FROM Airline WHERE airline_name = 'PP'")
status <- dbExecute(conn, "DELETE FROM Aircraft WHERE aircraft_name = 'CHENS-BIRD'")
status <- dbExecute(conn, "DELETE FROM Dep_Airport WHERE airport_name = 'PPP'")

## Check After Deleted 
sql <- "SELECT * FROM Incident WHERE iid = 'test1'"
rs <- dbGetQuery(conn, sql)
print(rs)

## Disconnect 
status <- dbDisconnect(conn)

