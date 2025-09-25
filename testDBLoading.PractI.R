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
## Count the number of unique airlines, flights, incidents, and first and 
## last dates from DF 

## Unique Airline Count 
unique_airline <- unique(df$airline)
count_unique_airlineCSV <- length(unique_airline)

## Unique Flights Count 
unique_flights <- unique(df$flight.number)
count_unique_flightsCSV <- length(unique_flights)

## Unique Incident Count 
unique_incident <- unique(df$iid)
count_unique_incidentCSV <- length(unique_incident)

## Oldest Date + Most Recent Date 
df$date <- as.Date(df$date, format = "%d.%m.%Y")

first_date <- min(df$date)
first_date <- as.Date(first_date, origin = "1970-01-01")
first_date <- format(first_date, "%Y-%m-%d")

last_date <- max(df$date)
last_date <- as.Date(last_date, origin = "1970-01-01")
last_date <- format(last_date, "%Y-%m-%d")

## Summary 
cat("CSV - Unique Airlines Count:", count_unique_airlineCSV,
    "\nCSV - Unique Flights Count:", count_unique_flightsCSV,
    "\nCSV - Unique Incidents Count:", count_unique_incidentCSV,
    "\nCSV - Oldest:", first_date,
    "\nCSV - Most Recent", last_date)

##------------------------------------------------------------------------------
## Counts the number of unique airlines, flights, incidents, and first and last 
## dates from our tables

## Distinct Airlines
sql <- "SELECT DISTINCT airline_name FROM Airline;"
AirlinesStatus <- dbGetQuery(conn, sql)
airlines_list <- AirlinesStatus$airline_name[AirlinesStatus != 'Unknown'
                                             & AirlinesStatus$airline_name != '']
count_unique_airlineSQL <- length(airlines_list)

## Distinct Flights
sql <- "SELECT DISTINCT flight_number AS Flights
        FROM Flights;"
FlightsStatus <- dbGetQuery(conn, sql)
flights_list <- FlightsStatus$Flights
count_unique_flightSQL <- length(flights_list)

## Distinct Incidents
sql <- "SELECT DISTINCT iid AS Incidents
        FROM Incident"
IncidentStatus <- dbGetQuery(conn, sql)
incident_list <- IncidentStatus$Incidents
count_unique_incidentSQL <- length(incident_list)

## Oldest Date
sql <- "SELECT MIN(STR_TO_DATE(date, '%d.%m.%Y')) AS Min_Date
        FROM Incident;"
MinDate <- dbGetQuery(conn, sql)
oldest <- MinDate$Min_Date

## Most Recent Date
sql <- "SELECT MAX(STR_TO_DATE(date, '%d.%m.%Y')) AS Max_Date
        FROM Incident;"
MaxDate <- dbGetQuery(conn, sql)
youngest <- MaxDate$Max_Date

## Summary 
cat("SQL - Unique Airlines Count: ", count_unique_airlineSQL,
    "\nSQL - Unique Flights Count: ", count_unique_flightSQL,
    "\nSQL - Unique Incidents Count: ",count_unique_incidentSQL,
    "\nSQL - Oldest: ", oldest,
    "\nSQL - Most Recent: ", youngest)
    
##------------------------------------------------------------------------------
## Extra Aggregates 
##------------------------------------------------------------------------------

## Average injuries 
AverageInjuriesCSV <- mean(df$num.injuries)
AverageInjuriesCSV <- round(AverageInjuriesCSV, 4)

sql <- "SELECT AVG(num_injuries) AS AverageInjuriesSQL FROM Incident"
suppressWarnings({
status1 <- dbGetQuery(conn, sql)
AverageInjuriesSQL <- round(status1$AverageInjuriesSQL, 4)
})

cat("In CSV:", AverageInjuriesCSV, "\nIn SQL:", AverageInjuriesSQL)

##------------------------------------------------------------------------------

## Average Delay 
DelayCSV <- mean(df$delay.mins)
DelayCSV <- round(DelayCSV, 4)

sql <- "SELECT AVG(delay_mins) AS AverageDelaySQL FROM Incident"
suppressWarnings({
status2 <- dbGetQuery(conn, sql)
AverageDelaySQL <- round(status2$AverageDelaySQL, 4) 
})
cat("In CSV:", DelayCSV, "\nIn SQL:", AverageDelaySQL)

##------------------------------------------------------------------------------
## Sum injuries 
SumInjuriesCSV <- sum(df$num.injuries)

sql <- "SELECT SUM(num_injuries) AS SumInjuriesSQL FROM Incident"
suppressWarnings({
status3 <- dbGetQuery(conn, sql)
})

cat("In CSV:", SumInjuriesCSV, "\nIn SQL:", status3$SumInjuriesSQL)

##------------------------------------------------------------------------------
## Sum Delay 
SumDelayCSV <- sum(df$delay.mins)

sql <- "SELECT SUM(delay_mins) AS SumDelaySQL FROM Incident"
suppressWarnings({
status4 <- dbGetQuery(conn, sql)
})
cat("In CSV:", SumDelayCSV, "\nIn SQL:", status4$SumDelaySQL)

##------------------------------------------------------------------------------
## Checking 
##------------------------------------------------------------------------------

if (count_unique_airlineSQL == count_unique_airlineCSV) {
  print("CSV and Tables have the same number of unique airlines")
} else {
  print("CSV and Tables do not have the same number of unique airlines")
}

if (count_unique_flightSQL == count_unique_flightsCSV) {
  print("CSV and Tables have the same number of unique flights")
} else {
  print("CSV and Tables do not have the same number of unique flights")
}

if (count_unique_incidentSQL == count_unique_incidentCSV) {
  print("CSV and Tables have the same number of unique incident")
} else {
  print("CSV and Tables do not have the same number of unique incident")
}

if (AverageInjuriesCSV == AverageInjuriesSQL) {
  print("CSV and Tables have the same average number of injuries")
} else {
  print("CSV and Tables do not have the average number of injuries")
}

if (DelayCSV == AverageDelaySQL) {
  print("CSV and Tables have the same average delay mins")
} else {
  print("CSV and Tables do not have the same average delay mins")
}

if (SumInjuriesCSV == status3$SumInjuriesSQL) {
  print("CSV and Tables have the same sum number of injuries")
} else {
  print("CSV and Tables do not have the same sum number of injuries")
}

if (SumDelayCSV == status4$SumDelaySQL) {
  print("CSV and Tables have the same sum of delay mins")
} else {
  print("CSV and Tables do not have the same sum of delay mins")
}

## Disconnect 
status <- dbDisconnect(conn)

