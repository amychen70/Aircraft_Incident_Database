##------------------------------------------------------------------------------

library(DBI)
library(RMySQL)

conn <- dbConnect(
  RMySQL::MySQL(),
  dbname = ,
  host = ,
  port = ,
  user = ,
  password = ,
)

##------------------------------------------------------------------------------

## Creates a lookup table for airline // David
status <- dbExecute(conn, "
  CREATE TABLE IF NOT EXISTS Airline (
  airline_name VARCHAR(50) PRIMARY KEY
  )
")
status <- dbExecute(conn, "
  INSERT IGNORE INTO Airline(airline_name) VALUES ('Unknown')
")


## Creates a lookup table for aircraft // David
status <- dbExecute(conn, "
  CREATE TABLE IF NOT EXISTS Aircraft (
  aircraft_name VARCHAR(50) PRIMARY KEY
  )
")
status <- dbExecute(conn, "
  INSERT IGNORE INTO Aircraft(aircraft_name) VALUES ('Unknown')
")

## Creates a lookup table for dep_airport // David
status <- dbExecute(conn, "
  CREATE TABLE IF NOT EXISTS Dep_Airport (
  airport_name VARCHAR(50) PRIMARY KEY
  )
")
status <- dbExecute(conn, "
  INSERT IGNORE INTO Dep_Airport(airport_name) VALUES ('Unknown')
")

## Creates a new Flights table // David
status <- dbExecute(conn, "
  CREATE TABLE IF NOT EXISTS Flights (
    fid VARCHAR(5) PRIMARY KEY NOT NULL,
    flight_number INT NOT NULL,
    airline VARCHAR(50) NOT NULL DEFAULT 'Unknown',
    aircraft VARCHAR(50) NOT NULL DEFAULT 'Unknown',
    dep_airport VARCHAR(50) NOT NULL DEFAULT 'Unknown',
    FOREIGN KEY (airline) REFERENCES Airline(airline_name),
    FOREIGN KEY (aircraft) REFERENCES Aircraft(aircraft_name),
    FOREIGN KEY (dep_airport) REFERENCES Dep_Airport(airport_name)
  );
")

##------------------------------------------------------------------------------

## Lookup for incident_type // Mei Qi (Amy) Chen 
status <- dbExecute(conn, "
  CREATE TABLE IF NOT EXISTS incidentLookup (
  incident_name VARCHAR(20) PRIMARY KEY 
  );
")
status <- dbExecute(conn, "
  INSERT IGNORE INTO incidentLookup(incident_name) VALUES ('Unknown')
")


## Lookup for severity // Mei Qi (Amy) Chen 
status <- dbExecute(conn, "
  CREATE TABLE IF NOT EXISTS severityLookup (
  severity_name VARCHAR(20) PRIMARY KEY 
  );
")
status <- dbExecute(conn, "
  INSERT IGNORE INTO severityLookup(severity_name) VALUES ('Unknown')
")


## Lookup for reported_by // Mei Qi (Amy) Chen 
status <- dbExecute(conn, "
  CREATE TABLE IF NOT EXISTS reportLookup (
  report_name VARCHAR(20) PRIMARY KEY 
  );
")
status <- dbExecute(conn, "
  INSERT IGNORE INTO reportLookup(report_name) VALUES ('Unknown')
")

## Incident Table // Mei Qi (Amy) Chen 
status <- dbExecute(conn, "
  CREATE TABLE IF NOT EXISTS Incident (
    iid VARCHAR(10) PRIMARY KEY,
    date VARCHAR(20) NOT NULL DEFAULT 'Unknown',
    incident_type VARCHAR(20) NOT NULL DEFAULT 'Unknown',
    severity VARCHAR(20) NOT NULL DEFAULT 'Unknown',
    delay_mins INTEGER NOT NULL DEFAULT 0,
    num_injuries INTEGER NOT NULL DEFAULT 0,
    reported_by VARCHAR(20) NOT NULL DEFAULT 'Unknown', 
    fid VARCHAR(5),
    FOREIGN KEY (fid) REFERENCES Flights(fid),
    FOREIGN KEY (incident_type) REFERENCES incidentLookup(incident_name),
    FOREIGN KEY (severity) REFERENCES severityLookup(severity_name),
    FOREIGN KEY (reported_by) REFERENCES reportLookup(report_name)
  );
")

##------------------------------------------------------------------------------
## Check table existent 
dbListTables(conn)

## Disconnect 
status <- dbDisconnect(conn)
