<!----------------------------------------------------------------------------->
<!----------------------------------------------------------------------------->
## Version 1.0.0

- Implemented `dbColumnInfo()` for `ClickHouseHTTPResult`: returns a
  data frame with `name` and `type` columns describing the R types of a
  query result. Works for both `Arrow` and `TabSeparatedWithNamesAndTypes`
  formats by parsing the buffered response content without consuming the
  result cursor, so `dbFetch()` can still be called afterwards.

<!----------------------------------------------------------------------------->
<!----------------------------------------------------------------------------->
## Version 0.99.0

- Migrated HTTP backend from `httr` to `httr2`: all requests are now built
  using the httr2 request pipeline (`request()` → `req_headers()` →
  `req_options()` → `req_perform()`).
- The `reset_handle` parameter of `dbConnect()` now maps to the httr2/curl
  `fresh_connect` option instead of the former `httr::handle_reset()`. Its
  behaviour is unchanged: set to TRUE to force a new TCP connection for each
  request rather than reusing a pooled one.
- HTTP error responses are handled manually (via `.query_success()`) rather
  than relying on httr2's automatic error raising, preserving existing
  ClickHouse exception-code detection logic.
- Minimum R version bumped to 4.1 (required by the native pipe `|>` used in
  the httr2 request pipeline).

<!----------------------------------------------------------------------------->
<!----------------------------------------------------------------------------->
## Version 0.3.5

- Bug fix (identified and corrected by Claude): `Date` columns returned as
  integers when using `format="Arrow"`. Newer ClickHouse versions send `Date`
  as Arrow `date32` directly, but the internal type-casting logic contained a
  `Date32 → int32` rule that was only ever intended as an intermediate step for
  old ClickHouse (which sent `Date` as `UInt16`). When applied to an already
  correctly typed `date32` column, it stripped the date semantics before
  conversion to R, yielding an integer instead of a `Date`. Fixed by rewriting
  the Arrow schema cast helpers (`result.R`) to generate the two-step
  `UInt16 → int32 → date32` chain only when the source column is actually
  `UInt16`, leaving `date32` and `timestamp` columns untouched.

<!----------------------------------------------------------------------------->
<!----------------------------------------------------------------------------->
## Version 0.3.4

- Don't use session by default
- Allow specifying database when manipulating tables

<!----------------------------------------------------------------------------->
<!----------------------------------------------------------------------------->
## Version 0.3.3

- Managing data type according to changes in new version of data.table (1.15.0)

<!----------------------------------------------------------------------------->
<!----------------------------------------------------------------------------->
## Version 0.3.2

- Add the possibility to use `httr::handle_reset()` when calling
`httr::POST()` to allow several independent connections in the same session:
when needed, set the "reset_handle" parameter to TRUE when calling `dbConnect()`

<!----------------------------------------------------------------------------->
<!----------------------------------------------------------------------------->
## Version 0.3.0

- `extended_headers` parameter in `dbConnect()`
- check current ClickHouse user in `dbConnect()`
- New contributor: https://github.com/eusebiu

<!----------------------------------------------------------------------------->
<!----------------------------------------------------------------------------->
## Version 0.2.0

`path` parameter in `dbConnect()` => use of a reverse proxy

<!----------------------------------------------------------------------------->
<!----------------------------------------------------------------------------->
## Version 0.1.3

`quote=""` when calling `data.table::fread()`

<!----------------------------------------------------------------------------->
<!----------------------------------------------------------------------------->
## Version 0.1.2

Explicit references to functions from dependencies


<!----------------------------------------------------------------------------->
<!----------------------------------------------------------------------------->
## Version 0.1.1

Better support of "TabSeparatedWithNamesAndTypes" format

<!----------------------------------------------------------------------------->
<!----------------------------------------------------------------------------->
## Version 0.1.0

- First version
