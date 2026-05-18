README
================

- [Installation](#installation)
  - [From CRAN](#from-cran)
  - [Dependencies](#dependencies)
  - [From github](#from-github)
- [Documentation](#documentation)
  - [Usage](#usage)
  - [Setting up a ClickHouse database using
    docker](#setting-up-a-clickhouse-database-using-docker)
- [Alternatives](#alternatives)
- [Acknowledgments](#acknowledgments)

[![CRAN_Status_Badge](https://www.r-pkg.org/badges/version/ClickHouseHTTP)](https://cran.r-project.org/package=ClickHouseHTTP)
[![](https://cranlogs.r-pkg.org/badges/ClickHouseHTTP)](https://cran.r-project.org/package=ClickHouseHTTP)

*ClickHouse* (<https://clickhouse.com/>) is an open-source, high
performance columnar OLAP (online analytical processing of queries)
database management system for real-time analytics using SQL. This *DBI*
backend relies on the ‘ClickHouse’ HTTP interface and support HTTPS
protocol.

This package has been developed as an alternative to the excellent
[RClickhouse](https://github.com/IMSMWU/RClickhouse) to provide secured
connection through SSL using HTTPS (unfortunately SSL connection is not
yet supported by RClickhouse).

The ClickHouseHTTP R package is licensed under
[GPL-3](https://www.gnu.org/licenses/gpl-3.0.en.html).

# Installation

## From CRAN

``` r
install.packages("ClickHouseHTTP")
```

## Dependencies

The following R packages available on CRAN are required:

- [methods](https://CRAN.R-project.org/package=methods): Formal Methods
  and Classes
- [DBI](https://CRAN.R-project.org/package=DBI): R Database Interface
- [httr2](https://CRAN.R-project.org/package=httr2): Perform HTTP
  Requests and Process the Responses
- [jsonlite](https://CRAN.R-project.org/package=jsonlite): A Simple and
  Robust JSON Parser and Generator for R
- [arrow](https://CRAN.R-project.org/package=arrow): Integration to
  ‘Apache’ ‘Arrow’
- [data.table](https://CRAN.R-project.org/package=data.table): Extension
  of `data.frame`
- [stats](https://CRAN.R-project.org/package=stats): The R Stats Package

And those are suggested:

- [knitr](https://CRAN.R-project.org/package=knitr): A General-Purpose
  Package for Dynamic Report Generation in R
- [rmarkdown](https://CRAN.R-project.org/package=rmarkdown): Dynamic
  Documents for R
- [dplyr](https://CRAN.R-project.org/package=dplyr): A Grammar of Data
  Manipulation
- [stringi](https://CRAN.R-project.org/package=stringi): Fast and
  Portable Character String Processing Facilities

## From github

``` r
devtools::install_github("patzaw/ClickHouseHTTP")
```

# Documentation

## Usage

### Connection

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

### Write a table in the database

``` r
library(dplyr)
data("mtcars")
mtcars <- as_tibble(mtcars, rownames = "car")
dbWriteTable(con, "mtcars", mtcars)
```

### Query the database

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
`format` argument of the `dbGetQuery()` function can be used to rely on
the *TabSeparatedWithNamesAndTypes* format.

``` r
selCars <- dbGetQuery(
  con,
  "SELECT car, mpg, cyl, hp FROM mtcars WHERE hp>=110",
  format = "TabSeparatedWithNamesAndTypes"
)
## Identifying the original ClickHouse data types
attr(selCars, "type")
```

### Using alternative databases stored in ClickHouse

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
chosen when connecting using the `dbname` argument of the `dbConnect()`
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

## Setting up a ClickHouse database using docker

### Configuration

``` sh
CH_VERSION=26.3.9.8
CH_HOME=~/Documents/Projects/Tests/Test_CH
mkdir -p $CH_HOME
mkdir -p ${CH_HOME}/data
mkdir -p ${CH_HOME}/conf
mkdir -p ${CH_HOME}/log

docker create --name temp_ch clickhouse/clickhouse-server:$CH_VERSION
docker cp temp_ch:/etc/clickhouse-server/. ${CH_HOME}/conf/
docker stop temp_ch
docker rm temp_ch
docker volume prune -f

echo '
<clickhouse>
    <users>
        <default>
            <password></password>
        </default>
    </users>
</clickhouse>
' > ${CH_HOME}/conf/users.d/default-password.xml
```

### SSL certificate

``` sh
openssl req -subj "/CN=localhost" -new -newkey rsa:2048 -days 365 -nodes -x509 -keyout ${CH_HOME}/conf/server.key -out ${CH_HOME}/conf/server.crt
openssl dhparam -out ${CH_HOME}/conf/dhparam.pem 2048

echo '
<clickhouse>
    <https_port>8443</https_port>
    <openSSL>
        <server>
            <certificateFile>/etc/clickhouse-server/server.crt</certificateFile>
            <privateKeyFile>/etc/clickhouse-server/server.key</privateKeyFile>
            <dhParamsFile>/etc/clickhouse-server/dhparam.pem</dhParamsFile>
            <verificationMode>none</verificationMode>
            <loadDefaultCAFile>true</loadDefaultCAFile>
            <cacheSessions>true</cacheSessions>
            <disableProtocols>sslv2,sslv3</disableProtocols>
            <preferServerCiphers>true</preferServerCiphers>
        </server>
    </openSSL>
</clickhouse>
' > ${CH_HOME}/conf/config.d/ssl.xml
```

### Container

The following ports are supported:

- 9000: Native TCP interface (not used by ClickHouseHTTP)
- 9440: Native TCP interface wrapped in TLS (not used by ClickHouseHTTP)
- 8123: HTTP interface
- 8443: HTTPS interface

``` sh
chmod -R a+rwx ${CH_HOME}
docker run -d --name Test_CH \
    --ulimit nofile=262144:262144 \
    --publish=9000:9000 \
    --publish=9440:9440 \
    --publish=8123:8123 \
    --publish=8443:8443 \
    --volume ${CH_HOME}/data:/var/lib/clickhouse \
    --volume ${CH_HOME}/conf:/etc/clickhouse-server \
    --volume ${CH_HOME}/log:/var/log/clickhouse-server \
    clickhouse/clickhouse-server:$CH_VERSION
```

# Alternatives

- [RClickhouse](https://github.com/IMSMWU/RClickhouse) is another DBI
  backend for the ClickHouse database. It provides basic dplyr support
  by auto-generating SQL-commands using dbplyr and is based on this [C++
  Clickhouse Client](https://github.com/artpaul/clickhouse-cpp).

- [clickhouse-r](https://github.com/hannes/clickhouse-r) is another DBI
  backend for the ClickHouse database relying on HTTP protocol. It
  provides SSL support but without peer verification for the moment.

# Acknowledgments

This work was supported by [UCB Pharma](https://www.ucb.com/) (Early
Solutions department).
