# Working with ClickHouse Databases Using ClickHouseHTTP

## Connection

``` r

library(DBI)
## HTTP connection
con <- dbConnect(
  ClickHouseHTTP::ClickHouseHTTP(),
  host = "localhost",
  port = 8123
)
## HTTPS connection (without ssl peer verification)
con <- dbConnect(
  ClickHouseHTTP::ClickHouseHTTP(),
  host = "localhost",
  port = 8443,
  https = TRUE,
  ssl_verifypeer = FALSE
)
```

## Write a table in the database

``` r

library(dplyr)
data("mtcars")
mtcars <- as_tibble(mtcars, rownames = "car")
dbWriteTable(con, "mtcars", mtcars)
```

## Query the database

``` r

carsFromDB <- dbReadTable(con, "mtcars")
dbGetQuery(con, "SELECT car, mpg, cyl, hp FROM mtcars WHERE hp>=110")
```

By default, ClickHouseHTTP relies on the [Apache
`Arrow`](https://arrow.apache.org/) format provided by ClickHouse.
However, as described in the
[documentation](https://clickhouse.com/docs/en/interfaces/formats/#data-format-arrow),
the following types are not supported in the current implementation of
this format: *TIME32*, *FIXED_SIZE_BINARY*, *JSON*, *UUID*, *ENUM*. The
`format` argument of the
[`dbGetQuery()`](https://dbi.r-dbi.org/reference/dbGetQuery.html)
function can be used to rely on the *TabSeparatedWithNamesAndTypes*
format.

``` r

selCars <- dbGetQuery(
  con,
  "SELECT car, mpg, cyl, hp FROM mtcars WHERE hp>=110",
  format = "TabSeparatedWithNamesAndTypes"
)
## Identifying the original ClickHouse data types
attr(selCars, "type")
```

## Using alternative databases stored in ClickHouse

It’s only possible when sessions are activated with the `use_session`
param.

``` r

library(DBI)
con <- dbConnect(
  ClickHouseHTTP::ClickHouseHTTP(),
  host = "localhost",
  port = 8123,
  use_session = TRUE
)
```

``` r

dbSendQuery(con, "CREATE DATABASE swiss")
dbSendQuery(con, "USE swiss")
```

The chosen database is used until the session expires. It can also be
chosen when connecting using the `dbname` argument of the
[`dbConnect()`](https://dbi.r-dbi.org/reference/dbConnect.html)
function.

The example below shows that spaces in column names are supported. It
also shows the support of R `list` using the *Array* ClickHouse type.

``` r

data("swiss")
swiss <- as_tibble(swiss, rownames = "province")
swiss <- mutate(swiss, "pr letters" = strsplit(province, ""))
dbWriteTable(
  conn = con,
  name = "swiss",
  value = swiss,
  engine = "MergeTree() ORDER BY (Fertility, province)"
)
swissFromDB <- dbReadTable(con, "swiss") |>
  as_tibble()
```

A table from another database can also be accessed as following:

``` r

dbReadTable(con, SQL("default.mtcars"))
```
