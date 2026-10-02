# Driver for the ClickHouse database using HTTP(S) interface

Driver for the ClickHouse database using HTTP(S) interface

Connect to a ClickHouse database using the ClickHouseHTTP DBI

## Usage

``` r
# S4 method for class 'ClickHouseHTTPDriver'
dbConnect(
  drv,
  host = "localhost",
  port = 8123L,
  dbname = "default",
  user = "default",
  password = "",
  https = FALSE,
  ssl_verifypeer = TRUE,
  host_path = NA,
  use_session = FALSE,
  session_timeout = 3600L,
  convert_uint = TRUE,
  extended_headers = list(),
  reset_handle = FALSE,
  settings = list(),
  ...
)
```

## Arguments

- drv:

  A driver object created by
  [`ClickHouseHTTP()`](https://patzaw.github.io/neo2R/reference/ClickHouseHTTP.md)

- host:

  name of the database host (default: "localhost")

- port:

  port on which the database is listening (default: 8123L)

- dbname:

  name of the default database (default: "default")

- user:

  user name (default: "default")

- password:

  user password (default: "")

- https:

  a logical to use the HTTPS protocol (default: FALSE)

- ssl_verifypeer:

  a logical to verify SSL certificate when using HTTPS (default: TRUE)

- host_path:

  a path to use on host (e.g. "ClickHouse/"): it allows to connect on a
  server behind a reverse proxy for example

- use_session:

  a logical indicating if a session should be created and use in
  ClickHouse (default: FALSE)

- session_timeout:

  timeout in seconds (default: 3600L seconds)

- convert_uint:

  a logical: if TRUE (default), UInt ClickHouse data types are converted
  in the following R classes:

  - UInt8 : logical

  - UInt16: Date (when using Arrow format in
    [dbSendQuery,ClickHouseHTTPConnection,character-method](https://patzaw.github.io/neo2R/reference/ClickHouseHTTPConnection-class.md))

  - UInt32: POSIXct (when using Arrow format in
    [dbSendQuery,ClickHouseHTTPConnection,character-method](https://patzaw.github.io/neo2R/reference/ClickHouseHTTPConnection-class.md))

- extended_headers:

  a named list with other HTTP headers (for example:
  `extended_headers=list("X-Authorization"="Bearer <token>")` can be
  used for OAuth access delegation)

- reset_handle:

  a logical indicating whether to force a fresh TCP connection for each
  request (httr2 `fresh_connect` curl option). If TRUE, connections are
  never reused from the pool, which can help in long-running sessions
  where stale connections cause errors (default: FALSE).

- settings:

  list of [Clickhouse
  settings](https://clickhouse.com/docs/en/operations/settings/settings/)

- ...:

  Other parameters passed on to methods

## Value

A ClickHouseHTTPConnection

## See also

[ClickHouseHTTPConnection](https://patzaw.github.io/neo2R/reference/ClickHouseHTTPConnection-class.md)

## Examples

``` r
if (FALSE) {
  ## Connection ----

  library(DBI)
  ### HTTP connection ----

  con <- dbConnect(
    ClickHouseHTTP::ClickHouseHTTP(),
    host = "localhost",
    port = 8123
  )

  ### HTTPS connection (without ssl peer verification) ----

  con <- dbConnect(
    ClickHouseHTTP::ClickHouseHTTP(),
    host = "localhost",
    port = 8443,
    https = TRUE,
    ssl_verifypeer = FALSE
  )

  ## Write a table in the database ----

  library(dplyr)
  data("mtcars")
  mtcars <- as_tibble(mtcars, rownames = "car")
  dbWriteTable(con, "mtcars", mtcars)

  ## Query the database ----

  carsFromDB <- dbReadTable(con, "mtcars")
  dbGetQuery(con, "SELECT car, mpg, cyl, hp FROM mtcars WHERE hp>=110")

  ## By default, ClickHouseHTTP relies on the
  ## Apache Arrow format provided by ClickHouse.
  ## The `format` argument of the `dbGetQuery()` function can be used to
  ## rely on the *TabSeparatedWithNamesAndTypes* format.
  selCars <- dbGetQuery(
    con,
    "SELECT car, mpg, cyl, hp FROM mtcars WHERE hp>=110",
    format = "TabSeparatedWithNamesAndTypes"
  )
  ## Identifying the original ClickHouse data types
  attr(selCars, "types")

  ## Using alternative databases stored in ClickHouse ----

  dbSendQuery(con, "CREATE DATABASE swiss")
  dbSendQuery(con, "USE swiss")

  ## The chosen database is used until the session expires.
  ## It can also be chosen when connecting using the `dbname` argument of
  ## the `dbConnect()` function.

  ## The example below shows that spaces in column names are supported.
  ## It also shows the support of R `list` using the *Array* ClickHouse type.
  data("swiss")
  swiss <- as_tibble(swiss, rownames = "province")
  swiss <- mutate(swiss, "pr letters" = strsplit(province, ""))
  dbWriteTable(
    con,
    "swiss",
    swiss,
    engine = "MergeTree() ORDER BY (Fertility, province)"
  )
  swissFromDB <- dbReadTable(con, "swiss")

  ## A table from another database can also be accessed as following:
  dbReadTable(con, SQL("default.mtcars"))
}
```
