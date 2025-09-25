##------------------------------------------------------------------------------

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

##------------------------------------------------------------------------------

url <- "https://s3.us-east-2.amazonaws.com/artificium.us/datasets/incidents.csv"
df <- read.csv(url, stringsAsFactors = F, header = T)

##------------------------------------------------------------------------------

## Inserting by batch
batch_size <- 500

## Insert values into Airline // David Li

for (i in seq(1, nrow(df), by = batch_size)) {
  batch <- df[i:min(i + batch_size - 1, nrow(df)), ]
  value_strings <- paste0("('", batch$airline, "')")
  sql <- paste0("INSERT IGNORE INTO Airline (airline_name) VALUES ", 
                paste(value_strings, collapse = ", "))
  
  status <- dbExecute(conn, sql)
}

## Insert values into Aircraft // David Li
for (i in seq(1, nrow(df), by = batch_size)) {
  batch <- df[i:min(i + batch_size - 1, nrow(df)), ]
  value_strings <- paste0("('", batch$aircraft, "')")
  sql <- paste0("INSERT IGNORE INTO Aircraft (aircraft_name) VALUES ", 
                paste(value_strings, collapse = ", "))
  
  status <- dbExecute(conn, sql)
}

## Insert values into Dep_Airport // David Li
for (i in seq(1, nrow(df), by = batch_size)) {
  batch <- df[i:min(i + batch_size - 1, nrow(df)), ]
  value_strings <- paste0("('", batch$dep.airport, "')")
  sql <- paste0("INSERT IGNORE INTO Dep_Airport (airport_name) VALUES ", 
                paste(value_strings, collapse = ", "))
  
  status <- dbExecute(conn, sql)
}

## Insert values into Flights // David Li
for (i in seq(1, nrow(df), by = batch_size)) {
  batch <- df[i:min(i + batch_size - 1, nrow(df)), ]
  
  value_strings <- paste0(
    "(",
    seq(i, i + nrow(batch) - 1), ", '",
    batch$flight.number, "', '",
    batch$airline, "', '",
    batch$aircraft, "', '",
    batch$dep.airport, "')"
  )
  
  sql <- paste0(
    "INSERT INTO Flights VALUES ",
    paste(value_strings, collapse = ", ")
  )
  status <- dbExecute(conn, sql)
}

##------------------------------------------------------------------------------

## Inserting by batch
batch_size <- 500

## Insert into incidentLookup // Mei Qi (Amy) Chen 
for (i in seq(1, nrow(df), by = batch_size)) {
  batch <- df[i:min(i + batch_size - 1, nrow(df)), ]
  value_strings <- paste0("('", batch$incident.type, "')")
  sql <- paste0("INSERT IGNORE INTO incidentLookup VALUES ", 
                paste(value_strings, collapse = ", "))
  
  status <- dbExecute(conn, sql) 
    
}

## Insert into severityLookup // Mei Qi (Amy) Chen 
for (i in seq(1, nrow(df), by = batch_size)) {
  batch <- df[i:min(i + batch_size - 1, nrow(df)), ]
  value_strings <- paste0("('", batch$severity, "')")
  sql <- paste0("INSERT IGNORE INTO severityLookup VALUES ",
                paste(value_strings, collapse = ", "))
  
  status <- dbExecute(conn, sql) 
}

## Insert into reportLookup // Mei Qi (Amy) Chen 
for (i in seq(1, nrow(df), by = batch_size)) {
  batch <- df[i:min(i + batch_size - 1, nrow(df)), ]
  value_strings <- paste0("('", batch$reported.by, "')")
  sql <- paste0("INSERT IGNORE INTO reportLookup VALUES ", 
                paste(value_strings, collapse = ", "))
  
  status <- dbExecute(conn, sql) 
}

## Insert values into Incident // Mei Qi (Amy) Chen 
for (i in seq(1, nrow(df), by = batch_size)) {
  batch <- df[i:min(i + batch_size - 1, nrow(df)), ]
  
  value_strings <- paste0(
    "('",
    batch$iid,  "', '", 
    batch$date,  "', '", 
    batch$incident.type,  "', '", 
    batch$severity, "', ", 
    batch$delay.mins,  ", ", 
    batch$num.injuries,  ", '",
    batch$reported.by,  "', ", 
    seq(i, i + nrow(batch) - 1), ")"
  )
  
  sql <- paste0(
    "INSERT INTO Incident VALUES ",
    paste(value_strings, collapse = ", ")
  )
  
  status <- dbExecute(conn, sql)
}

##------------------------------------------------------------------------------

## Checkings 
dbGetQuery(conn, "SELECT * FROM Flights LIMIT 10")
dbGetQuery(conn, "SELECT * FROM Incident LIMIT 10")
dbGetQuery(conn, "SELECT COUNT(*) FROM Flights")
dbGetQuery(conn, "SELECT COUNT(*) FROM Incident")

## Disconnect 
status <- dbDisconnect(conn)
