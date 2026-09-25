###############################################################################@
## dbplyr_edition ----
##
## Declares support for dbplyr's 2nd edition interface so that dplyr::tbl()
## and other dbplyr-based verbs work with ClickHouseHTTPConnection objects.
## Registered as an S3 method for dbplyr's generic without requiring dbplyr
## to be attached or hard-imported.
#' @exportS3Method dbplyr::dbplyr_edition
dbplyr_edition.ClickHouseHTTPConnection <- function(con) {
  2L
}

###############################################################################@
## sql_translation ----
##
## dbplyr's default (ANSI) translation targets cast type names (TEXT,
## INTEGER, NUMERIC...), function names (POWER) and aggregate functions (sd,
## var) that ClickHouse does not recognize, and leaves Sys.Date()/Sys.time()
## untranslated entirely. This overrides just those entries -- using
## ClickHouse's own conversion functions (toInt64, toFloat64, toString,
## toDate, toDateTime, toUInt8) rather than CAST(... AS type), and its
## native function names (pow, today, now, stddevSamp, varSamp) -- on top of
## dbplyr's base translators for everything else.
##
## Where useful this mirrors choices made by the RClickhouse package (e.g.
## toInt64 rather than a narrower 32-bit cast, to avoid overflow), but is an
## independent implementation: RClickhouse is GPL-2 (version 2 only) while
## this package is GPL-3 (version 3 only), and those two licenses are not
## compatible for combining code, so nothing is copied from it. Two of its
## choices are deliberately not mirrored: it disables all window functions
## (`base_no_win`), which was likely needed for the ClickHouse versions of
## its time but is no longer necessary now that ClickHouse has native window
## function support; and its `Sys.date` translation entry (lower-case "d")
## can never match a call to `Sys.Date()`, so it has no effect.
##
## RClickhouse also ships dbplyr_case_sensitive()/fix_dbplyr(), which
## monkey-patch dbplyr's *shared* internal sql_prefix() via
## utils::assignInNamespace() so that untranslated function calls keep their
## original case (ClickHouse function names are case-sensitive, e.g.
## toStartOfMonth()). That's global, session-wide state -- it would affect
## every connection/backend in the session, not just ClickHouse ones. It's
## also unnecessary here: dbplyr stopped uppercasing untranslated function
## names in dbplyr 1.4.0 (2019, dbplyr#181), well before the dbplyr (>= 2.0.0)
## RClickhouse itself requires, so untranslated calls already keep their
## original case per-connection with no monkey-patching needed.
#' @exportS3Method dbplyr::sql_translation
sql_translation.ClickHouseHTTPConnection <- function(con) {
  dbplyr::sql_variant(
    scalar = dbplyr::sql_translator(
      .parent = dbplyr::base_scalar,
      `^` = dbplyr::sql_prefix("pow"),
      as.character = dbplyr::sql_prefix("toString"),
      as.integer = dbplyr::sql_prefix("toInt64"),
      as.double = dbplyr::sql_prefix("toFloat64"),
      as.numeric = dbplyr::sql_prefix("toFloat64"),
      as.logical = dbplyr::sql_prefix("toUInt8"),
      as.Date = dbplyr::sql_prefix("toDate"),
      as_date = dbplyr::sql_prefix("toDate"),
      as.POSIXct = dbplyr::sql_prefix("toDateTime"),
      as_datetime = dbplyr::sql_prefix("toDateTime"),
      Sys.Date = dbplyr::sql_prefix("today", 0),
      Sys.time = dbplyr::sql_prefix("now", 0)
    ),
    aggregate = dbplyr::sql_translator(
      .parent = dbplyr::base_agg,
      sd = dbplyr::sql_aggregate("stddevSamp", "sd"),
      var = dbplyr::sql_aggregate("varSamp", "var")
    ),
    window = dbplyr::sql_translator(
      .parent = dbplyr::base_win,
      sd = dbplyr::win_aggregate("stddevSamp"),
      var = dbplyr::win_aggregate("varSamp")
    )
  )
}

###############################################################################@
## sql_table_analyze ----
##
## dbplyr's default builds and executes an "ANALYZE <table>" statement after
## copy_to()/compute(), which ClickHouse does not support. Returning NULL
## (rather than e.g. TRUE) is the documented way to skip it -- see
## ?dbplyr::sql_table_analyze.
#' @exportS3Method dbplyr::sql_table_analyze
sql_table_analyze.ClickHouseHTTPConnection <- function(con, table, ...) {
  NULL
}

###############################################################################@
## db_connection_describe ----
##
## Used by dbplyr for the "# Source:" header when printing a tbl(); the
## default just prints the connection's class name. This shows the
## ClickHouse server identity instead, reusing dbGetInfo().
#' @exportS3Method dbplyr::db_connection_describe
db_connection_describe.ClickHouseHTTPConnection <- function(con, ...) {
  info <- DBI::dbGetInfo(con)
  sprintf(
    "clickhouse %s [%s@%s:%s/%s]",
    info$db.version,
    info$username,
    info$host,
    info$port,
    info$dbname
  )
}
