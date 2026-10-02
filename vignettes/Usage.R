## ----setup, include = FALSE------------------------------------------------------------------------------------------------------------------------------------------
library(knitr)
library(ClickHouseHTTP)
library(dplyr)

## ----eval=FALSE------------------------------------------------------------------------------------------------------------------------------------------------------
# library(DBI)
# ## HTTP connection
# con <- dbConnect(
#   ClickHouseHTTP::ClickHouseHTTP(),
#   host = "localhost",
#   port = 8123
# )
# ## HTTPS connection (without ssl peer verification)
# con <- dbConnect(
#   ClickHouseHTTP::ClickHouseHTTP(),
#   host = "localhost",
#   port = 8443,
#   https = TRUE,
#   ssl_verifypeer = FALSE
# )

## ----eval=FALSE------------------------------------------------------------------------------------------------------------------------------------------------------
# library(dplyr)
# data("mtcars")
# mtcars <- as_tibble(mtcars, rownames = "car")
# dbWriteTable(con, "mtcars", mtcars)

## ----eval=FALSE------------------------------------------------------------------------------------------------------------------------------------------------------
# carsFromDB <- dbReadTable(con, "mtcars")
# dbGetQuery(con, "SELECT car, mpg, cyl, hp FROM mtcars WHERE hp>=110")

## ----eval=FALSE------------------------------------------------------------------------------------------------------------------------------------------------------
# selCars <- dbGetQuery(
#   con,
#   "SELECT car, mpg, cyl, hp FROM mtcars WHERE hp>=110",
#   format = "TabSeparatedWithNamesAndTypes"
# )
# ## Identifying the original ClickHouse data types
# attr(selCars, "type")

## ----eval=FALSE------------------------------------------------------------------------------------------------------------------------------------------------------
# library(DBI)
# con <- dbConnect(
#   ClickHouseHTTP::ClickHouseHTTP(),
#   host = "localhost",
#   port = 8123,
#   use_session = TRUE
# )

## ----eval=FALSE------------------------------------------------------------------------------------------------------------------------------------------------------
# dbSendQuery(con, "CREATE DATABASE swiss")
# dbSendQuery(con, "USE swiss")

## ----eval=FALSE------------------------------------------------------------------------------------------------------------------------------------------------------
# data("swiss")
# swiss <- as_tibble(swiss, rownames = "province")
# swiss <- mutate(swiss, "pr letters" = strsplit(province, ""))
# dbWriteTable(
#   conn = con,
#   name = "swiss",
#   value = swiss,
#   engine = "MergeTree() ORDER BY (Fertility, province)"
# )
# swissFromDB <- dbReadTable(con, "swiss") |>
#   as_tibble()

## ----eval=FALSE------------------------------------------------------------------------------------------------------------------------------------------------------
# dbReadTable(con, SQL("default.mtcars"))

