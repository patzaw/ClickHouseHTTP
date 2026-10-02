# ClickHouseHTTPConnection class.

ClickHouseHTTPConnection class.

Send SQL query to ClickHouse

Information about the ClickHouse database

List tables in ClickHouse

Does a table exist?

Read database tables as data frames

List field names of a table

Remove a table from the database

Create a table in ClickHouse

Insert rows into a table

Write a table in ClickHouse

## Usage

``` r
# S4 method for class 'ClickHouseHTTPConnection,character'
dbSendQuery(
  conn,
  statement,
  format = c("Arrow", "TabSeparatedWithNamesAndTypes"),
  file = NA,
  ...
)

# S4 method for class 'ClickHouseHTTPConnection'
dbGetInfo(dbObj, ...)

# S4 method for class 'ClickHouseHTTPConnection'
dbListTables(conn, database = NA, ...)

# S4 method for class 'ClickHouseHTTPConnection,character'
dbExistsTable(conn, name, database = NA, ...)

# S4 method for class 'ClickHouseHTTPConnection,character'
dbReadTable(conn, name, database = NA, ...)

# S4 method for class 'ClickHouseHTTPConnection,character'
dbListFields(conn, name, database = NA, ...)

# S4 method for class 'ClickHouseHTTPConnection,ANY'
dbRemoveTable(conn, name, database = NA, ...)

# S4 method for class 'ClickHouseHTTPConnection'
dbCreateTable(
  conn,
  name,
  fields,
  database = NA,
  engine = "TinyLog",
  overwrite = FALSE,
  ...,
  row.names = NULL,
  temporary = FALSE
)

# S4 method for class 'ClickHouseHTTPConnection'
dbAppendTable(conn, name, value, database = NA, ..., row.names = NULL)

# S4 method for class 'ClickHouseHTTPConnection,ANY'
dbWriteTable(
  conn,
  name,
  value,
  database = NA,
  overwrite = FALSE,
  append = FALSE,
  engine = "TinyLog",
  ...
)
```

## Arguments

- conn:

  a ClickHouseHTTPConnection object created with
  [`dbConnect()`](https://patzaw.github.io/neo2R/reference/ClickHouseHTTPDriver-class.md)

- statement:

  the SQL query statement

- format:

  the format used by ClickHouse to send the results. Two formats are
  supported: "Arrow" (default) and "TabSeparatedWithNamesAndTypes"

- file:

  a path to a file to send along the query (default: NA)

- ...:

  Other parameters passed on to methods

- dbObj:

  a ClickHouseHTTPConnection object

- database:

  the database to consider. If NA (default), the default database or the
  one in use in the session (if a session is defined).

- name:

  the name of the table to create

- fields:

  a character vector with the name of the fields and their ClickHouse
  type (e.g.
  `c("text_col String", "num_col Nullable(Float64)", "nul_col Array(Int32)")`
  )

- engine:

  the ClickHouse table engine as described in ClickHouse
  [documentation](https://clickhouse.com/docs/en/engines/table-engines/).
  Examples:

  - `"TinyLog"` (default)

  - `"MergeTree() ORDER BY (expr)"` (expr generally correspond to fields
    separated by ",")

- overwrite:

  if TRUE and if a table with the same name exists, then it is deleted
  before creating the new one (default: FALSE)

- row.names:

  unsupported parameter (add for compatibility reason)

- temporary:

  unsupported parameter (add for compatibility reason)

- value:

  a data.frame

- append:

  if TRUE, the values are added to the database table if it exists
  (default: FALSE).

## Value

A ClickHouseHTTPResult object

A list with the following elements:

- name: "ClickHouseHTTPConnection"

- db.version: the version of ClickHouse

- uptime: ClickHouse uptime

- dbname: the default database

- username: user name

- host: ClickHouse host

- port: ClickHouse port

- https: Is the connection using HTTPS protocol instead of HTTP

A vector of character with table names.

A logical.

A data.frame.

A vector of character with column names.

`invisible(TRUE)`

dbCreateTable() returns TRUE, invisibly.

The number of rows written, invisibly.

TRUE; called for side effects

## Details

Both format have their pros and cons:

- **Arrow** (default):

  - fast for long tables but slow for wide tables

  - fast with Array columns

  - Date and DateTime columns are returned as UInt16 and UInt32
    respectively: by default, ClickHouseHTTP interpret them as Date and
    POSIXct columns but cannot make the difference with actual UInt16
    and UInt32

- **TabSeparatedWithNamesAndTypes**:

  - in general faster than Arrow

  - fast for wide tables but slow for long tables

  - slow with Array columns

  - Special characters are not well interpreted. In such cases, the
    function below can be useful but can also take time.

          .sp_ch_recov <- function(x){
             stringi::stri_replace_all_regex(
                x,
                c(
                   "\\n", "\\t",  "\\r", "\\b",
                   "\\a", "\\f", "\\'",  "\\\\"
                ),
                c("\n", "\t", "\r", "\b", "\a", "\f", "'", "\\"),
                vectorize_all=FALSE
             )
          }

## Note

Time zones: `toTimeZone()` calls in SQL queries are ignored when
`format = "Arrow"` and are not correctly interpreted when
`format = "TabSeparatedWithNamesAndTypes"`. It is preferable to set the
time zone through the `settings` parameter of
[`DBI::dbConnect()`](https://dbi.r-dbi.org/reference/dbConnect.html),
for example `settings = list(session_timezone = "CET")`.

## See also

[ClickHouseHTTPResult](https://patzaw.github.io/neo2R/reference/ClickHouseHTTPResult-class.md)

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
