# Offline dummy connection ---------------------------------------------------
# An S4 instance of ClickHouseHTTPConnection with no network activity, only
# used so dbplyr's S3 methods (dbplyr_edition, sql_translation) dispatch and
# so lazy_frame()/sql_render() can be exercised without a live server.
ch_dummy_con <- function() {
  methods::new(
    "ClickHouseHTTPConnection",
    host = "localhost",
    port = 8123L,
    user = "default",
    password = function() "",
    https = FALSE,
    ssl_verifypeer = FALSE,
    host_path = "",
    session = "",
    convert_uint = FALSE,
    extended_headers = list(),
    reset_handle = FALSE,
    settings = ""
  )
}

# Live connection, for round-trip tests ---------------------------------------
# Configured entirely through environment variables so CI/dev machines can
# opt in without touching test code:
#   CLICKHOUSE_TEST_HOST      (required to enable live tests)
#   CLICKHOUSE_TEST_PORT      (default 8123)
#   CLICKHOUSE_TEST_HTTPS     (default FALSE)
#   CLICKHOUSE_TEST_HOST_PATH (default "")
#   CLICKHOUSE_TEST_USER      (default "default")
#   CLICKHOUSE_TEST_PASSWORD  (default "")
.ch_test_con_cache <- new.env(parent = emptyenv())

ch_test_con <- function() {
  if (!is.null(.ch_test_con_cache$con)) {
    return(.ch_test_con_cache$con)
  }
  host <- Sys.getenv("CLICKHOUSE_TEST_HOST", "")
  if (identical(host, "")) {
    return(NULL)
  }
  con <- tryCatch(
    DBI::dbConnect(
      ClickHouseHTTP::ClickHouseHTTP(),
      host = host,
      port = as.integer(Sys.getenv("CLICKHOUSE_TEST_PORT", "8123")),
      user = Sys.getenv("CLICKHOUSE_TEST_USER", "default"),
      password = Sys.getenv("CLICKHOUSE_TEST_PASSWORD", ""),
      https = as.logical(Sys.getenv("CLICKHOUSE_TEST_HTTPS", "FALSE")),
      ssl_verifypeer = as.logical(Sys.getenv(
        "CLICKHOUSE_TEST_SSL_VERIFYPEER",
        "TRUE"
      )),
      host_path = Sys.getenv("CLICKHOUSE_TEST_HOST_PATH", NA)
    ),
    error = function(e) NULL
  )
  .ch_test_con_cache$con <- con
  con
}

skip_if_no_clickhouse <- function() {
  testthat::skip_if(
    is.null(ch_test_con()),
    "No live ClickHouse test server configured (set CLICKHOUSE_TEST_HOST)"
  )
}

# Uncached connection with custom `settings` (e.g. session_timezone), for
# tests that need something other than the shared ch_test_con(). Not cached
# since different tests need different settings; caller should disconnect it
# (e.g. with withr::defer(DBI::dbDisconnect(con))).
ch_test_con_with_settings <- function(settings) {
  host <- Sys.getenv("CLICKHOUSE_TEST_HOST", "")
  if (identical(host, "")) {
    return(NULL)
  }
  tryCatch(
    DBI::dbConnect(
      ClickHouseHTTP::ClickHouseHTTP(),
      host = host,
      port = as.integer(Sys.getenv("CLICKHOUSE_TEST_PORT", "8123")),
      user = Sys.getenv("CLICKHOUSE_TEST_USER", "default"),
      password = Sys.getenv("CLICKHOUSE_TEST_PASSWORD", ""),
      https = as.logical(Sys.getenv("CLICKHOUSE_TEST_HTTPS", "FALSE")),
      ssl_verifypeer = as.logical(Sys.getenv(
        "CLICKHOUSE_TEST_SSL_VERIFYPEER",
        "TRUE"
      )),
      host_path = Sys.getenv("CLICKHOUSE_TEST_HOST_PATH", NA),
      settings = settings
    ),
    error = function(e) NULL
  )
}
