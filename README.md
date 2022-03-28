README
================

-   [1 Installation](#installation)
    -   [1.1 Dependencies](#dependencies)
    -   [1.2 From github](#from-github)
-   [2 Documentation](#documentation)
    -   [2.1 Usage](#usage)
    -   [2.2 Setting up a ClickHouse database using
        docker](#setting-up-a-clickhouse-database-using-docker)
-   [3 Alternatives](#alternatives)
-   [4 Acknowledgments](#acknowledgments)

[![CRAN_Status_Badge](http://www.r-pkg.org/badges/version/ClickHouseHTTP)](https://cran.r-project.org/package=ClickHouseHTTP)
[![](http://cranlogs.r-pkg.org/badges/ClickHouseHTTP)](https://cran.r-project.org/package=ClickHouseHTTP)

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

# 1 Installation

## 1.1 Dependencies

The following R packages available on CRAN are required:

-   [methods](https://CRAN.R-project.org/package=methods): Formal
    Methods and Classes
-   [DBI](https://CRAN.R-project.org/package=DBI): R Database Interface
-   [httr](https://CRAN.R-project.org/package=httr): Tools for Working
    with URLs and HTTP
-   [jsonlite](https://CRAN.R-project.org/package=jsonlite): A Simple
    and Robust JSON Parser and Generator for R
-   [arrow](https://CRAN.R-project.org/package=arrow): Integration to
    ‘Apache’ ‘Arrow’

## 1.2 From github

``` r
devtools::install_github("patzaw/ClickHouseHTTP")
```

# 2 Documentation

## 2.1 Usage

### 2.1.1 Connection

``` r
library(DBI)
## HTTP connection
con <- dbConnect(
   ClickHouseHTTP::ClickHouseHTTP(), host="localhost",
   port=8123
)
## HTTPS connection (without ssl peer verification)
con <- dbConnect(
   ClickHouseHTTP::ClickHouseHTTP(), host="localhost",
   port=8443, https=TRUE, ssl_verifypeer=FALSE
)
```

### 2.1.2 Write a table in the database

``` r
library(dplyr)
data("mtcars")
mtcars <- as_tibble(mtcars, rownames="car")
dbWriteTable(con, "mtcars", mtcars)
```

### 2.1.3 Query the database

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
   con, "SELECT car, mpg, cyl, hp FROM mtcars WHERE hp>=110",
   format="TabSeparatedWithNamesAndTypes"
)
## Identifying the original ClickHouse data types
attr(selCars, "type")
```

### 2.1.4 Using alternative databases stored in ClickHouse

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
swiss <- as_tibble(swiss, rownames="province")
swiss <- mutate(swiss, "pr letters"=strsplit(province, ""))
dbWriteTable(
   con, "swiss", swiss,
   engine="MergeTree() ORDER BY (Fertility, province)"
)
swissFromDB <- dbReadTable(con, "swiss")
```

A table from another database can also be accessed as following:

``` r
dbReadTable(con, SQL("default.mtcars"))
```

## 2.2 Setting up a ClickHouse database using docker

### 2.2.1 Configuration

``` sh
CH_HOME=~/Documents/Projects/Test_CH
mkdir -p $CH_HOME
mkdir -p ${CH_HOME}/data
mkdir -p ${CH_HOME}/conf
mkdir -p ${CH_HOME}/log
```

Configuration files are shared in the `supp/ClickHouse-Conf-Files`
folder in this repository.

``` sh
cp supp/ClickHouse-Conf-Files/users.xml ${CH_HOME}/conf/
cp supp/ClickHouse-Conf-Files/config.xml ${CH_HOME}/conf/
```

### 2.2.2 SSL certificate

``` sh
openssl req -subj "/CN=localhost" -new -newkey rsa:2048 -days 365 -nodes -x509 -keyout ${CH_HOME}/conf/server.key -out ${CH_HOME}/conf/server.crt
openssl dhparam -out ${CH_HOME}/conf/dhparam.pem 4096
```

### 2.2.3 Container

The following ports are supported in the shared `config.xml` file:

-   9000: Native TCP interface (not used by ClickHouseHTTP)
-   9440: Native TCP interface wrapped in TLS (not used by
    ClickHouseHTTP)
-   8123: HTTP interface
-   8443: HTTPS interface

``` sh
chmod -R a+rwx ${CH_HOME}
docker run -d --name Test_CH \
    --ulimit nofile=262144:262144 \
    --volume ${CH_HOME}/data:/var/lib/clickhouse \
    --publish=9000:9000 \
    --publish=9440:9440 \
    --publish=8123:8123 \
    --publish=8443:8443 \
    --volume ${CH_HOME}/conf/users.xml:/etc/clickhouse-server/users.xml \
    --volume ${CH_HOME}/conf/config.xml:/etc/clickhouse-server/config.xml \
    --volume ${CH_HOME}/conf/server.crt:/etc/clickhouse-server/server.crt \
    --volume ${CH_HOME}/conf/server.key:/etc/clickhouse-server/server.key \
    --volume ${CH_HOME}/conf/dhparam.pem:/etc/clickhouse-server/dhparam.pem \
    --volume ${CH_HOME}/log:/var/log/clickhouse-server \
    clickhouse/clickhouse-server:22.2.3.5
```

# 3 Alternatives

-   [RClickhouse](https://github.com/IMSMWU/RClickhouse) is another DBI
    backend for the ClickHouse database. It provides basic dplyr support
    by auto-generating SQL-commands using dbplyr and is based on this
    [C++ Clickhouse Client](https://github.com/artpaul/clickhouse-cpp).

-   [clickhouse-r](https://github.com/hannes/clickhouse-r) is another
    DBI backend for the ClickHouse database relying on HTTP protocol. It
    provides SSL support but without peer verification for the moment.

# 4 Acknowledgments

This work was entirely supported by [UCB Pharma](https://www.ucb.com/)
(Early Solutions department).
