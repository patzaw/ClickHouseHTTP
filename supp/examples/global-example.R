\dontrun{

## Connection ----

library(ClickHouseHTTP)
### HTTP connection ----

con <- dbConnect(
   ClickHouseHTTP(), host="localhost",
   port=8123
)

### HTTPS connection (without ssl peer verification) ----

con <- dbConnect(
   ClickHouseHTTP(), host="localhost",
   port=8443, https=TRUE, ssl_verifypeer=FALSE
)

## Write a table in the database ----

library(dplyr)
data("mtcars")
mtcars <- as_tibble(mtcars, rownames="car")
dbWriteTable(con, "mtcars", mtcars)

## Query the database ----

carsFromDB <- DBI::dbReadTable(con, "mtcars")
DBI::dbGetQuery(con, "SELECT car, mpg, cyl, hp FROM mtcars WHERE hp>=110")

## By default, ClickHouseHTTP relies on the
## Apache Arrow format provided by ClickHouse.
## The `format` argument of the `dbGetQuery()` function can be used to
## rely on the *TabSeparatedWithNamesAndTypes* format.
selCars <- DBI::dbGetQuery(
   con, "SELECT car, mpg, cyl, hp FROM mtcars WHERE hp>=110",
   format="TabSeparatedWithNamesAndTypes"
)
## Identifying the original ClickHouse data types
attr(selCars, "type")

## Using alternative databases stored in ClickHouse ----

dbSendQuery(con, "CREATE DATABASE swiss")
dbSendQuery(con, "USE swiss")

## The chosen database is used until the session expires.
## It can also be chosen when connecting using the `dbname` argument of
## the `dbConnect()` function.

## The example below shows that spaces in column names are supported.
## It also shows the support of R `list` using the *Array* ClickHouse type.
data("swiss")
swiss <- as_tibble(swiss, rownames="province")
swiss <- mutate(swiss, "pr letters"=strsplit(province, ""))
dbWriteTable(
   con, "swiss", swiss,
   engine="MergeTree() ORDER BY (Fertility, province)"
)
swissFromDB <- DBI::dbReadTable(con, "swiss")

## A table from another database can also be accessed as following:
DBI::dbReadTable(con, DBI::SQL("default.mtcars"))

}
