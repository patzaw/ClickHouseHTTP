# Round-trip tests against a real ClickHouse server: they confirm the SQL
# dbplyr generates is not just well-formed (see test-dbplyr-sql-translation.R)
# but is actually accepted and correctly executed by ClickHouse.
#
# Skipped unless CLICKHOUSE_TEST_HOST is set (see helper-connection.R). To
# run locally: point CLICKHOUSE_TEST_HOST/PORT/... at a real or dockerized
# ClickHouse instance, e.g.
#   docker run -p 8123:8123 clickhouse/clickhouse-server
#   CLICKHOUSE_TEST_HOST=localhost Rscript -e "testthat::test_dir('tests/testthat')"

local_test_table <- function(con, name, df) {
  DBI::dbWriteTable(con, name, df, overwrite = TRUE)
  withr::defer(DBI::dbRemoveTable(con, name), envir = parent.frame())
  name
}

test_that("tbl()/collect() round-trips a simple table", {
  skip_if_no_clickhouse()
  skip_if_not_installed("withr")
  skip_if_not_installed("dbplyr")
  skip_if_not_installed("dplyr")
  con <- ch_test_con()

  df <- data.frame(a = c(1, 2, 3), s = c("x", "y", "z"))
  tbl_name <- local_test_table(con, "chhttp_test_dbplyr_basic", df)

  out <- dplyr::tbl(con, tbl_name) |>
    dplyr::filter(a > 1) |>
    dplyr::select(a, s) |>
    dplyr::arrange(a) |>
    dplyr::collect()

  expect_equal(out$a, c(2, 3))
  expect_equal(out$s, c("y", "z"))
})

test_that("group_by()/summarise() aggregates match local dplyr", {
  skip_if_no_clickhouse()
  skip_if_not_installed("withr")
  skip_if_not_installed("dbplyr")
  skip_if_not_installed("dplyr")
  con <- ch_test_con()

  df <- data.frame(
    grp = c("a", "a", "b", "b", "b"),
    val = c(1, 2, 3, 4, 5)
  )
  tbl_name <- local_test_table(con, "chhttp_test_dbplyr_agg", df)

  remote <- dplyr::tbl(con, tbl_name) |>
    dplyr::group_by(grp) |>
    dplyr::summarise(
      n = dplyr::n(),
      total = sum(val, na.rm = TRUE),
      avg = mean(val, na.rm = TRUE),
      sdv = sd(val, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::arrange(grp) |>
    dplyr::collect()

  local <- df |>
    dplyr::group_by(grp) |>
    dplyr::summarise(
      n = dplyr::n(),
      total = sum(val, na.rm = TRUE),
      avg = mean(val, na.rm = TRUE),
      sdv = sd(val, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::arrange(grp)

  expect_equal(as.data.frame(remote), as.data.frame(local))
})

test_that("as.*() cast translations produce correct values", {
  skip_if_no_clickhouse()
  skip_if_not_installed("withr")
  skip_if_not_installed("dbplyr")
  skip_if_not_installed("dplyr")
  con <- ch_test_con()

  df <- data.frame(a = 1L, s = "42")
  tbl_name <- local_test_table(con, "chhttp_test_dbplyr_cast", df)

  out <- dplyr::tbl(con, tbl_name) |>
    dplyr::mutate(
      cs = as.character(a),
      ni = as.integer(s),
      nd = as.numeric(s)
    ) |>
    dplyr::collect()

  expect_equal(out$cs, "1")
  expect_equal(out$ni, 42L)
  expect_equal(out$nd, 42)
})
