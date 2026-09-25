# Round-trip tests for DateTime/timezone handling against a real ClickHouse
# server (see supp/local_tests/time.R for the manual script these formalize).
# Skipped unless CLICKHOUSE_TEST_HOST is set (see helper-connection.R).

test_that("now() uses the session_timezone setting (TabSeparatedWithNamesAndTypes)", {
  skip_if_no_clickhouse()
  skip_if_not_installed("withr")
  con <- ch_test_con_with_settings(list(session_timezone = "EST"))
  skip_if(is.null(con), "Could not open a connection with custom settings")
  withr::defer(DBI::dbDisconnect(con))

  out <- DBI::dbGetQuery(
    con,
    "SELECT now() AS T, 'hello' AS H",
    format = "TabSeparatedWithNamesAndTypes"
  )

  expect_s3_class(out$T, "POSIXct")
  expect_identical(attr(out$T, "tzone"), "EST")
  expect_equal(out$H, "hello")
  ## the wall-clock string ClickHouse sent was in EST; interpreting it back
  ## with tz="EST" must recover the same instant as the local clock.
  expect_lt(abs(as.numeric(difftime(out$T, Sys.time(), units = "secs"))), 30)
})

test_that("an explicit column timezone overrides session_timezone (TabSeparatedWithNamesAndTypes)", {
  skip_if_no_clickhouse()
  skip_if_not_installed("withr")
  con <- ch_test_con_with_settings(list(session_timezone = "EST"))
  skip_if(is.null(con), "Could not open a connection with custom settings")
  withr::defer(DBI::dbDisconnect(con))

  out <- DBI::dbGetQuery(
    con,
    "SELECT toTimeZone(now(), 'UTC') AS T, 'hello' AS H",
    format = "TabSeparatedWithNamesAndTypes"
  )

  expect_s3_class(out$T, "POSIXct")
  expect_identical(attr(out$T, "tzone"), "UTC")
  expect_equal(out$H, "hello")
  expect_lt(abs(as.numeric(difftime(out$T, Sys.time(), units = "secs"))), 30)
})

test_that("now() with no session_timezone returns a naive UTC POSIXct (TabSeparatedWithNamesAndTypes)", {
  skip_if_no_clickhouse()
  con <- ch_test_con()

  out <- DBI::dbGetQuery(
    con,
    "SELECT now() AS T, 'hello' AS H",
    format = "TabSeparatedWithNamesAndTypes"
  )

  expect_s3_class(out$T, "POSIXct")
  expect_null(attr(out$T, "tzone"))
  expect_equal(out$H, "hello")
  expect_lt(abs(as.numeric(difftime(out$T, Sys.time(), units = "secs"))), 30)
})

test_that("Arrow and TabSeparatedWithNamesAndTypes agree on the instant for now() under session_timezone", {
  skip_if_no_clickhouse()
  skip_if_not_installed("withr")
  con <- ch_test_con_with_settings(list(session_timezone = "EST"))
  skip_if(is.null(con), "Could not open a connection with custom settings")
  withr::defer(DBI::dbDisconnect(con))

  arrow_out <- DBI::dbGetQuery(con, "SELECT now() AS T")
  tsv_out <- DBI::dbGetQuery(
    con,
    "SELECT now() AS T",
    format = "TabSeparatedWithNamesAndTypes"
  )

  expect_lt(
    abs(as.numeric(difftime(arrow_out$T, tsv_out$T, units = "secs"))),
    5
  )
})
