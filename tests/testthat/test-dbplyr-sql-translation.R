# These tests render SQL from dbplyr lazy queries against a dummy
# ClickHouseHTTPConnection: no network is involved, they only check that
# dbplyr generates ClickHouse-compatible SQL text. They do NOT verify that
# ClickHouse actually accepts/executes it -- see test-dbplyr-live.R for that.

test_that("dbplyr_edition declares 2nd edition support", {
  skip_if_not_installed("dbplyr")
  expect_equal(dbplyr::dbplyr_edition(ch_dummy_con()), 2L)
})

test_that("dplyr::tbl() no longer hits the 1st-edition error", {
  skip_if_not_installed("dbplyr")
  expect_no_error(dbplyr:::check_2ed(ch_dummy_con()))
})

test_that("as.*() casts use ClickHouse native conversion functions", {
  skip_if_not_installed("dbplyr")
  skip_if_not_installed("dplyr")
  lf <- dbplyr::lazy_frame(a = 1, s = "x", con = ch_dummy_con())

  sql <- dbplyr::sql_render(
    dplyr::mutate(
      lf,
      cs = as.character(a),
      ni = as.integer(a),
      nd = as.numeric(a),
      nl = as.logical(a),
      da = as.Date(s),
      pp = as.POSIXct(s)
    )
  )

  expect_match(as.character(sql), "toString(`a`)", fixed = TRUE)
  expect_match(as.character(sql), "toInt64(`a`)", fixed = TRUE)
  expect_match(as.character(sql), "toFloat64(`a`)", fixed = TRUE)
  expect_match(as.character(sql), "toUInt8(`a`)", fixed = TRUE)
  expect_match(as.character(sql), "toDate(`s`)", fixed = TRUE)
  expect_match(as.character(sql), "toDateTime(`s`)", fixed = TRUE)
})

test_that("^ uses ClickHouse's pow(), Sys.Date()/Sys.time() are translated", {
  skip_if_not_installed("dbplyr")
  skip_if_not_installed("dplyr")
  lf <- dbplyr::lazy_frame(a = 1, con = ch_dummy_con())

  pow_sql <- as.character(dbplyr::sql_render(dplyr::mutate(lf, p = a^2)))
  expect_match(pow_sql, "pow(`a`, 2.0)", fixed = TRUE)

  time_sql <- as.character(dbplyr::sql_render(
    dplyr::mutate(lf, t = Sys.Date(), n = Sys.time())
  ))
  expect_match(time_sql, "today()", fixed = TRUE)
  expect_match(time_sql, "now()", fixed = TRUE)
})

test_that("sql_table_analyze() is a no-op (ClickHouse has no ANALYZE)", {
  skip_if_not_installed("dbplyr")
  expect_null(dbplyr::sql_table_analyze(ch_dummy_con(), dbplyr::ident("t")))
})

test_that("db_connection_describe() reports the ClickHouse server, not just the class name", {
  skip_if_not_installed("dbplyr")
  # Uses a stub with a canned dbGetInfo() so this stays offline.
  setClass("ChTestConn", contains = "ClickHouseHTTPConnection")
  setMethod("dbGetInfo", "ChTestConn", function(dbObj, ...) {
    list(
      db.version = "24.1",
      username = "default",
      host = "localhost",
      port = 8123L,
      dbname = "default"
    )
  })
  con <- methods::new("ChTestConn", ch_dummy_con())

  expect_equal(
    dbplyr::db_connection_describe(con),
    "clickhouse 24.1 [default@localhost:8123/default]"
  )
})

test_that("sd()/var() map to ClickHouse's stddevSamp/varSamp", {
  skip_if_not_installed("dbplyr")
  skip_if_not_installed("dplyr")
  lf <- dbplyr::lazy_frame(a = 1, s = "x", con = ch_dummy_con())

  agg_sql <- dbplyr::sql_render(
    dplyr::summarise(
      dplyr::group_by(lf, s),
      sdv = sd(a, na.rm = TRUE),
      vv = var(a, na.rm = TRUE)
    )
  )
  expect_match(as.character(agg_sql), "stddevSamp(`a`)", fixed = TRUE)
  expect_match(as.character(agg_sql), "varSamp(`a`)", fixed = TRUE)

  win_sql <- dbplyr::sql_render(
    dplyr::mutate(dplyr::group_by(lf, s), sdv = sd(a, na.rm = TRUE))
  )
  expect_match(as.character(win_sql), "stddevSamp(`a`) OVER", fixed = TRUE)
})

test_that("baseline verbs render stable SQL (regression guard)", {
  skip_if_not_installed("dbplyr")
  skip_if_not_installed("dplyr")
  lf <- dbplyr::lazy_frame(a = 1, b = 2, s = "x", con = ch_dummy_con())

  expect_equal(
    as.character(dbplyr::sql_render(
      dplyr::select(dplyr::filter(lf, a > 1), a, b)
    )),
    "SELECT `a`, `b`\nFROM `df`\nWHERE (`a` > 1.0)"
  )

  expect_equal(
    as.character(dbplyr::sql_render(dplyr::distinct(lf, s))),
    "SELECT DISTINCT `s`\nFROM `df`"
  )

  agg_sql <- as.character(dbplyr::sql_render(
    dplyr::summarise(
      dplyr::group_by(lf, s),
      n = dplyr::n(),
      m = mean(a, na.rm = TRUE),
      tot = sum(a, na.rm = TRUE)
    )
  ))
  expect_match(agg_sql, "COUNT(*)", fixed = TRUE)
  expect_match(agg_sql, "AVG(`a`)", fixed = TRUE)
  expect_match(agg_sql, "SUM(`a`)", fixed = TRUE)
})

test_that("known gap: regex string functions require fixed() patterns", {
  # dbplyr's default translation only supports *fixed* patterns unless a
  # backend explicitly declares regex support. ClickHouseHTTP does not yet
  # override str_detect()/str_replace()/etc., so non-fixed patterns still
  # error -- same as most other dbplyr backends out of the box. Use
  # stringr::fixed() or write raw SQL via dbplyr::sql() as a workaround.
  skip_if_not_installed("dbplyr")
  skip_if_not_installed("dplyr")
  skip_if_not_installed("stringr")
  lf <- dbplyr::lazy_frame(s = "x", con = ch_dummy_con())

  expect_error(
    dbplyr::sql_render(dplyr::mutate(lf, m = stringr::str_detect(s, "x"))),
    "fixed"
  )
})

test_that("known gap: wday() is not translated", {
  # R's wday() has label/abbr/week_start arguments with no direct SQL
  # equivalent; left unsupported rather than risk a silently wrong
  # day-numbering convention. Use ClickHouse's toDayOfWeek() via
  # dbplyr::sql() if needed.
  skip_if_not_installed("dbplyr")
  skip_if_not_installed("dplyr")
  lf <- dbplyr::lazy_frame(d = Sys.Date(), con = ch_dummy_con())

  expect_error(
    dbplyr::sql_render(dplyr::mutate(lf, wd = wday(d))),
    class = "dbplyr_error_unsupported_fn"
  )
})
