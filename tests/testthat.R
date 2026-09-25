library(testthat)
library(ClickHouseHTTP)

test_check("ClickHouseHTTP")

## If there is a ClickHouse database available for testing
# Sys.setenv(CLICKHOUSE_TEST_HOST="localhost")
# testthat::test_dir("tests/testthat")
