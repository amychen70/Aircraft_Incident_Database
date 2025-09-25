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

# Drop lookout table 
status <- dbExecute(conn, "DROP TABLE IF EXISTS Incident;")
status <- dbExecute(conn, "DROP TABLE IF EXISTS Flights;")

# Drop lookout table 
status <- dbExecute(conn, "DROP TABLE IF EXISTS incidentLookup")
status <- dbExecute(conn, "DROP TABLE IF EXISTS severityLookup")
status <- dbExecute(conn, "DROP TABLE IF EXISTS reportLookup")

# Drop lookup table 
status <- dbExecute(conn, "DROP TABLE IF EXISTS Airline")
status <- dbExecute(conn, "DROP TABLE IF EXISTS Aircraft")
status <- dbExecute(conn, "DROP TABLE IF EXISTS Dep_Airport")


status <- dbDisconnect(conn)
