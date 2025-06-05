###############################################################################@
## ClickHouseHTTPConnection----
#' ClickHouseHTTPConnection class.
#'
#' @export
#'
setClass(
  "ClickHouseHTTPConnection",
  contains = "DBIConnection",
  slots = list(
    host = "character",
    port = "integer",
    user = "character",
    password = "function",
    https = "logical",
    ssl_verifypeer = "logical",
    host_path = "character",
    session = "character",
    convert_uint = "logical",
    extended_headers = "list",
    reset_handle = "logical",
    settings = "character"
  )
)

###############################################################################@
## dbIsValid ----
##
setMethod(
  "dbIsValid",
  "ClickHouseHTTPConnection",
  function(dbObj, ...) {
    toRet <- try(.check_db_session(dbObj), silent = TRUE)
    if (inherits(toRet, "try-error")) {
      v <- FALSE
      attr(v, "m") <- as.character(toRet)
      toRet <- v
    }
    if (!toRet) {
      warning(attr(toRet, "m"))
      toRet <- as.logical(toRet)
    }
    return(toRet)
  }
)

###############################################################################@
## dbDisconnect ----
##
setMethod(
  "dbDisconnect",
  "ClickHouseHTTPConnection",
  function(conn, ...) {
    xn <- deparse(substitute(conn))
    conn@session <- paste0(conn@session, "off")
    assign(xn, conn, envir = parent.frame(n = 1))
    return(invisible(TRUE))
  }
)

###############################################################################@
## dbSendQuery ----
#' Send SQL query to ClickHouse
#'
#' @param conn a ClickHouseHTTPConnection object created
#' with [`dbConnect()`][ClickHouseHTTPDriver-class]
#' @param statement the SQL query statement
#' @param format the format used by ClickHouse to send the results.
#' Two formats are supported:
#' "Arrow" (default) and "TabSeparatedWithNamesAndTypes"
#' @param file a path to a file to send along the query (default: NA)
#' @param ... Other parameters passed on to methods
#'
#' @return A ClickHouseHTTPResult object
#'
#' @details Both format have their pros and cons:
#'
#' - **Arrow** (default):
#'    - fast for long tables but slow for wide tables
#'    - fast with Array columns
#'    - Date and DateTime columns are returned as UInt16 and UInt32
#'    respectively: by default, ClickHouseHTTP interpret them as Date and
#'    POSIXct columns but cannot make the difference with actual UInt16 and
#'    UInt32
#'
#' - **TabSeparatedWithNamesAndTypes**:
#'    - in general faster than Arrow
#'    - fast for wide tables but slow for long tables
#'    - slow with Array columns
#'    - Special characters are not well interpreted. In such cases, the function
#'    below can be useful but can also take time.
#'
#' \preformatted{
#'       .sp_ch_recov <- function(x){
#'          stringi::stri_replace_all_regex(
#'             x,
#'             c(
#'                "\\\\n", "\\\\t",  "\\\\r", "\\\\b",
#'                "\\\\a", "\\\\f", "\\\\'",  "\\\\\\\\"
#'             ),
#'             c("\n", "\t", "\r", "\b", "\a", "\f", "'", "\\\\"),
#'             vectorize_all=FALSE
#'          )
#'       }
#' }
#'
#' @example supp/examples/global-example.R
#'
#' @seealso [ClickHouseHTTPResult-class]
#'
#' @rdname ClickHouseHTTPConnection-class
#'
#' @export
#'
setMethod(
  "dbSendQuery",
  c("ClickHouseHTTPConnection", "character"),
  function(
    conn,
    statement,
    format = c("Arrow", "TabSeparatedWithNamesAndTypes"),
    file = NA,
    ...
  ) {
    format <- match.arg(format)
    resEnv <- new.env(parent = emptyenv())
    query <- statement
    if (
      length(grep(
        "^[[:space:]]*(SELECT|DESC|DESCRIBE|SHOW|WITH|EXISTS)[[:space:]]+",
        statement,
        value = T,
        ignore.case = TRUE
      )) ==
        1
    ) {
      query = paste(query, "FORMAT", format)
    }
    r <- .send_query(
      dbc = conn,
      query = query,
      file = file
    )
    v <- .query_success(r)
    if (!v) {
      m <- attr(v, "m")
      if (length(grep("Arrow data format", m)) > 0) {
        m <- paste0(
          m,
          '\n\n   Issue with Arrow data format: ',
          'try using format="TabSeparatedWithNamesAndTypes", ',
          'or converting unsupported data type in the query ',
          '(e.g. "SELECT toString(uuidColName) as uuidColName")'
        )
      }
      stop(m)
    }
    resEnv <- as.environment(r)
    resEnv$ch_summary <- lapply(
      jsonlite::fromJSON(r$headers$`x-clickhouse-summary`),
      as.numeric
    )
    resEnv$fetched <- FALSE
    return(new(
      "ClickHouseHTTPResult",
      sql = statement,
      env = resEnv,
      conn = conn,
      format = format
    ))
  }
)
###############################################################################@
## show ----
##
setMethod("show", "ClickHouseHTTPConnection", function(object) {
  cat("<ClickHouseHTTPConnection>\n")
  if (DBI::dbIsValid(object)) {
    cat(sprintf(
      "   %s://%s@%s:%s\n",
      ifelse(object@https, "https", "http"),
      object@user,
      object@host,
      object@port
    ))
  } else {
    cat("   DISCONNECTED\n")
  }
})

###############################################################################@
## dbGetInfo ----
#' Information about the ClickHouse database
#'
#' @param dbObj a ClickHouseHTTPConnection object
#'
#' @return A list with the following elements:
#' - name: "ClickHouseHTTPConnection"
#' - db.version: the version of ClickHouse
#' - uptime: ClickHouse uptime
#' - dbname: the default database
#' - username: user name
#' - host: ClickHouse host
#' - port: ClickHouse port
#' - https: Is the connection using HTTPS protocol instead of HTTP
#'
#' @rdname ClickHouseHTTPConnection-class
#'
#' @export
#'
setMethod(
  "dbGetInfo",
  "ClickHouseHTTPConnection",
  function(dbObj, ...) {
    chinfo <- DBI::dbGetQuery(
      dbObj,
      paste(
        "SELECT version() as version, uptime() as uptime",
        ", currentDatabase() as database"
      )
    )
    return(list(
      name = "ClickHouseHTTPConnection",
      db.version = chinfo$version,
      uptime = chinfo$uptime,
      dbname = chinfo$database,
      username = dbObj@user,
      host = dbObj@host,
      port = dbObj@port,
      https = dbObj@https
    ))
  }
)

###############################################################################@
## dbListTables ----
#' List tables in ClickHouse
#'
#' @param conn a ClickHouseHTTPConnection object created
#' with [`dbConnect()`][ClickHouseHTTPDriver-class]
#' @param database the database to consider. If NA (default), the default
#' database or  the one in use in the session (if a session is defined).
#' @param ... Other parameters passed on to methods
#'
#' @return A vector of character with table names.
#'
#' @rdname ClickHouseHTTPConnection-class
#'
setMethod(
  "dbListTables",
  "ClickHouseHTTPConnection",
  function(conn, database = NA, ...) {
    if (is.na(database)) {
      qdb <- ""
    } else {
      qdb <- paste(" FROM", DBI::dbQuoteIdentifier(conn, database))
    }
    return(
      as.character(DBI::dbGetQuery(
        conn,
        sprintf(
          "SHOW TABLES%s",
          qdb
        )
      )[[1]])
    )
  }
)

###############################################################################@
## dbDataType ----
##
setMethod(
  "dbDataType",
  "ClickHouseHTTPConnection",
  function(dbObj, obj, ...) {
    return(DBI::dbDataType(ClickHouseHTTP(), obj))
  },
  valueClass = "character"
)

###############################################################################@
## dbQuoteIdentifier.character ----
##
setMethod(
  "dbQuoteIdentifier",
  c("ClickHouseHTTPConnection", "character"),
  function(conn, x, ...) {
    if (anyNA(x)) {
      stop("Input to dbQuoteIdentifier must not contain NA.")
    } else {
      x <- gsub('\\', '\\\\', x, fixed = TRUE)
      x <- gsub('`', '\\`', x, fixed = TRUE)
      return(DBI::SQL(paste0('`', x, '`')))
    }
  }
)

###############################################################################@
## dbQuoteIdentifier.SQL ----
##
setMethod(
  "dbQuoteIdentifier",
  c("ClickHouseHTTPConnection", "SQL"),
  function(conn, x, ...) {
    return(x)
  }
)

###############################################################################@
## dbQuoteString.character ----
##
setMethod(
  "dbQuoteString",
  c("ClickHouseHTTPConnection", "character"),
  function(conn, x, ...) {
    x <- gsub('\\', '\\\\', x, fixed = TRUE)
    x <- gsub("'", "\\'", x, fixed = TRUE)
    return(DBI::SQL(ifelse(is.na(x), "NULL", paste0("'", x, "'"))))
  }
)

###############################################################################@
## dbQuoteString.SQL ----
##
setMethod(
  "dbQuoteString",
  c("ClickHouseHTTPConnection", "SQL"),
  function(conn, x, ...) {
    return(x)
  }
)

###############################################################################@
## dbExistsTable ----
#' Does a table exist?
#'
#' @param conn a ClickHouseHTTPConnection object created
#' with [`dbConnect()`][ClickHouseHTTPDriver-class]
#' @param name the table name
#' @param database the database to consider. If NA (default), the default
#' database or  the one in use in the session (if a session is defined).
#' @param ... Other parameters passed on to methods
#'
#' @return A logical.
#'
#' @rdname ClickHouseHTTPConnection-class
#'
setMethod(
  "dbExistsTable",
  c("ClickHouseHTTPConnection", "character"),
  function(conn, name, database = NA, ...) {
    qname <- DBI::dbQuoteIdentifier(conn, name)
    if (is.na(database)) {
      qdb <- ""
    } else {
      qdb <- paste0(DBI::dbQuoteIdentifier(conn, database), ".")
    }
    return(DBI::dbGetQuery(
      conn,
      sprintf(
        "EXISTS %s%s",
        qdb,
        qname
      )
    )[[1]])
  }
)

###############################################################################@
## dbReadTable ----
#' Read database tables as data frames
#'
#' @param conn a ClickHouseHTTPConnection object created
#' with [`dbConnect()`][ClickHouseHTTPDriver-class]
#' @param name the table name
#' @param database the database to consider. If NA (default), the default
#' database or  the one in use in the session (if a session is defined).
#' @param ... Other parameters passed on to methods
#'
#' @return A data.frame.
#'
#' @rdname ClickHouseHTTPConnection-class
#'
setMethod(
  "dbReadTable",
  c("ClickHouseHTTPConnection", "character"),
  function(conn, name, database = NA, ...) {
    qname <- DBI::dbQuoteIdentifier(conn, name)
    if (is.na(database)) {
      qdb <- ""
    } else {
      qdb <- paste0(DBI::dbQuoteIdentifier(conn, database), ".")
    }
    ## Identify UUID columns for converting them in String
    coltype <- DBI::dbGetQuery(
      conn,
      paste0("DESCRIBE TABLE ", qdb, qname)
    )[, c("name", "type")]
    if (any(coltype$type == "UUID")) {
      query <- paste(
        "SELECT",
        paste(
          ifelse(
            coltype$type == "UUID",
            sprintf(
              "toString(%s) as %s",
              DBI::dbQuoteIdentifier(conn, coltype$name),
              DBI::dbQuoteIdentifier(conn, coltype$name)
            ),
            DBI::dbQuoteIdentifier(conn, coltype$name)
          ),
          collapse = ", "
        )
      )
    } else {
      query <- "SELECT *"
    }
    return(DBI::dbGetQuery(
      conn,
      paste(query, "FROM ", qdb, qname),
      ...
    ))
  }
)

###############################################################################@
## dbListFields ----
#' List field names of a table
#'
#' @param conn a ClickHouseHTTPConnection object created
#' with [`dbConnect()`][ClickHouseHTTPDriver-class]
#' @param name the table name
#' @param database the database to consider. If NA (default), the default
#' database or  the one in use in the session (if a session is defined).
#' @param ... Other parameters passed on to methods
#'
#' @return A vector of character with column names.
#'
#' @rdname ClickHouseHTTPConnection-class
#'
setMethod(
  "dbListFields",
  c("ClickHouseHTTPConnection", "character"),
  function(conn, name, database = NA, ...) {
    qname <- DBI::dbQuoteIdentifier(conn, name)
    if (is.na(database)) {
      qdb <- ""
    } else {
      qdb <- paste0(DBI::dbQuoteIdentifier(conn, database), ".")
    }
    return(DBI::dbGetQuery(conn, paste0("DESCRIBE TABLE ", qdb, qname))$name)
  }
)

###############################################################################@
## dbRemoveTable ----
#' Remove a table from the database
#'
#' @param conn a ClickHouseHTTPConnection object created
#' with [`dbConnect()`][ClickHouseHTTPDriver-class]
#' @param name the table name
#' @param database the database to consider. If NA (default), the default
#' database or  the one in use in the session (if a session is defined).
#' @param ... Other parameters passed on to methods
#'
#' @return `invisible(TRUE)`
#'
#' @rdname ClickHouseHTTPConnection-class
#'
setMethod(
  "dbRemoveTable",
  "ClickHouseHTTPConnection",
  function(conn, name, database = NA, ...) {
    qname <- DBI::dbQuoteIdentifier(conn, name)
    if (is.na(database)) {
      qdb <- ""
    } else {
      qdb <- paste0(DBI::dbQuoteIdentifier(conn, database), ".")
    }
    DBI::dbSendQuery(conn, paste0("DROP TABLE ", qdb, qname))
    return(invisible(TRUE))
  }
)

###############################################################################@
## dbCreateTable ----
#' Create a table in ClickHouse
#'
#' @param conn a ClickHouseHTTPConnection object created
#' with [`dbConnect()`][ClickHouseHTTPDriver-class]
#' @param name the name of the table to create
#' @param database the database to consider. If NA (default), the default
#' database or  the one in use in the session (if a session is defined).
#' @param fields a character vector with the name of the fields and their
#' ClickHouse type
#' (e.g.
#' `c("text_col String", "num_col Nullable(Float64)", "nul_col Array(Int32)")`
#' )
#' @param engine the ClickHouse table engine as described in ClickHouse
#' [documentation](https://clickhouse.com/docs/en/engines/table-engines/).
#' Examples:
#' - `"TinyLog"` (default)
#' - `"MergeTree() ORDER BY (expr)"`
#' (expr generally correspond to fields separated by ",")
#' @param overwrite if TRUE and if a table with the same name exists,
#' then it is deleted before creating the new one (default: FALSE)
#' @param row.names unsupported parameter (add for compatibility reason)
#' @param temporary unsupported parameter (add for compatibility reason)
#' @param ... for compatibility purpose; not used
#'
#' @return dbCreateTable() returns TRUE, invisibly.
#'
#' @example supp/examples/global-example.R
#'
#' @rdname ClickHouseHTTPConnection-class
#'
#' @export
#'
setMethod(
  "dbCreateTable",
  "ClickHouseHTTPConnection",
  function(
    conn,
    name,
    database = NA,
    fields,
    engine = "TinyLog",
    overwrite = FALSE,
    ...,
    row.names = NULL,
    temporary = FALSE
  ) {
    qname <- DBI::dbQuoteIdentifier(conn, name)
    if (overwrite && DBI::dbExistsTable(conn, qname)) {
      DBI::dbRemoveTable(conn, qname, database = database)
    }
    if (is.na(database)) {
      qdb <- ""
    } else {
      qdb <- paste0(DBI::dbQuoteIdentifier(conn, database), ".")
    }
    query <- paste(
      sprintf(
        "CREATE TABLE %s (",
        paste0(qdb, qname)
      ),
      paste(fields, collapse = ",\n"),
      ") ENGINE =",
      engine
    )

    DBI::dbSendQuery(conn, query)
    return(invisible(TRUE))
  }
)

###############################################################################@
## dbAppendTable ----
#' Insert rows into a table
#'
#' @param conn a ClickHouseHTTPConnection object created
#' with [`dbConnect()`][ClickHouseHTTPDriver-class]
#' @param name the table name
#' @param database the database to consider. If NA (default), the default
#' database or  the one in use in the session (if a session is defined).
#' @param value a data.frame
#' @param row.names unsupported parameter (add for compatibility reason)
#' @param ... Other parameters passed on to methods
#'
#' @return `invisible(TRUE)`
#'
#' @rdname ClickHouseHTTPConnection-class
#'
setMethod(
  "dbAppendTable",
  "ClickHouseHTTPConnection",
  function(
    conn,
    name,
    database = NA,
    value,
    ...,
    row.names = NULL
  ) {
    qname <- DBI::dbQuoteIdentifier(conn, name)
    if (is.na(database)) {
      qdb <- ""
    } else {
      qdb <- paste0(DBI::dbQuoteIdentifier(conn, database), ".")
    }
    tmpf <- tempfile()
    on.exit(file.remove(tmpf))
    arrow::write_feather(value, tmpf)
    query <- sprintf("INSERT INTO %s FORMAT Arrow", paste0(qdb, qname))
    res <- DBI::dbSendQuery(conn = conn, statement = query, file = tmpf)
    return(invisible(DBI::dbGetRowsAffected(res)))
  }
)

###############################################################################@
## dbWriteTable ----
#' Write a table in ClickHouse
#'
#' @param conn a ClickHouseHTTPConnection object created
#' with [`dbConnect()`][ClickHouseHTTPDriver-class]
#' @param name the name of the table to create
#' @param database the database to consider. If NA (default), the default
#' database or  the one in use in the session (if a session is defined).
#' @param value a data.frame
#' @param overwrite if TRUE and if a table with the same name exists,
#' then it is deleted before creating the new one (default: FALSE)
#' @param append if TRUE, the values are added to the database table if
#' it exists (default: FALSE).
#' @param engine the ClickHouse table engine as described in ClickHouse
#' [documentation](https://clickhouse.com/docs/en/engines/table-engines/).
#' Examples:
#' - `"TinyLog"` (default)
#' - `"MergeTree() ORDER BY (expr)"`
#' (expr generally correspond to fields separated by ",")
#' @param ... Other parameters passed on to methods
#'
#' @return TRUE; called for side effects
#'
#' @example supp/examples/global-example.R
#'
#' @rdname ClickHouseHTTPConnection-class
#'
#' @export
#'
setMethod(
  "dbWriteTable",
  "ClickHouseHTTPConnection",
  function(
    conn,
    name,
    database = NA,
    value,
    overwrite = FALSE,
    append = FALSE,
    engine = "TinyLog",
    ...
  ) {
    if (overwrite && append) {
      stop("overwrite and append cannot be both TRUE")
    }
    if (!append) {
      cdef <- unlist(lapply(
        value,
        function(x) {
          type <- DBI::dbDataType(conn, x)
          if (any(is.na(x))) {
            type <- sprintf("Nullable(%s)", type)
          }
          return(type)
        }
      ))
      fields <- paste(DBI::dbQuoteIdentifier(conn, names(cdef)), cdef)
      DBI::dbCreateTable(
        conn = conn,
        name = name,
        database = database,
        fields = fields,
        engine = engine,
        overwrite = overwrite
      )
    }
    toRet <- DBI::dbAppendTable(
      conn = conn,
      name = name,
      database = database,
      value = value
    )
    return(invisible(toRet))
  }
)

###############################################################################@
## Helpers ----

.build_http_req <- function(
  host,
  port,
  https,
  host_path,
  session,
  session_timeout = NA,
  settings,
  query = ""
) {
  use_session <- !is.na(session)
  if (use_session) {
    to_ret <- sprintf(
      "%s://%s:%s/%s?session_id=%s&session_check=%s%s%s%s",
      ifelse(https, "https", "http"),
      host,
      port,
      ifelse(is.na(host_path), "", paste0(host_path, "/")),
      session,
      as.integer(is.na(session_timeout)),
      ifelse(
        is.na(session_timeout),
        "",
        sprintf("&session_timeout=%s", as.integer(session_timeout))
      ),
      ifelse(
        settings == "",
        "",
        paste0("&", settings)
      ),
      ifelse(
        is.na(query) || query == "",
        "",
        sprintf("&query=%s", utils::URLencode(query))
      )
    )
  } else {
    to_ret <- sprintf(
      "%s://%s:%s/%s%s%s",
      ifelse(https, "https", "http"),
      host,
      port,
      ifelse(is.na(host_path), "", paste0(host_path, "/")),
      ifelse(
        settings == "",
        "",
        paste0("?", settings)
      ),
      ifelse(
        is.na(query) || query == "",
        "",
        sprintf(
          "%squery=%s",
          ifelse(
            settings == "",
            "?",
            "&"
          ),
          utils::URLencode(query)
        )
      )
    )
  }
  return(to_ret)
}

.send_query <- function(
  dbc,
  query,
  file = NA,
  session_timeout = NA
) {
  if (is.na(file)) {
    qbody <- query
    query <- ""
  } else {
    qbody <- httr::upload_file(file)
    query <- utils::URLencode(query)
  }

  ## Add authentication headers ----
  qheaders <- list(
    'X-ClickHouse-User' = dbc@user,
    'X-ClickHouse-Key' = dbc@password()
  )
  ## Add other headers ----
  qheaders <- c(qheaders, dbc@extended_headers)

  url <- .build_http_req(
    host = dbc@host,
    port = dbc@port,
    https = dbc@https,
    host_path = dbc@host_path,
    session = dbc@session,
    session_timeout = session_timeout,
    settings = dbc@settings,
    query = query
  )
  if (dbc@reset_handle) {
    httr::POST(
      url = url,
      body = qbody,
      do.call(httr::add_headers, qheaders),
      config = httr::config(ssl_verifypeer = as.integer(dbc@ssl_verifypeer)),
      handle = httr::handle_reset(url)
    )
  } else {
    httr::POST(
      url = url,
      body = qbody,
      do.call(httr::add_headers, qheaders),
      config = httr::config(ssl_verifypeer = as.integer(dbc@ssl_verifypeer))
    )
  }
}

.query_success <- function(r) {
  if (
    r$status_code >= 300 ||
      !is.null(r$headers$`x-clickhouse-exception-code`)
  ) {
    if (!is.null(r$headers$`x-clickhouse-exception-code`)) {
      m <- rawToChar(r$content)
    } else {
      m <- sprintf(
        "Connection error. Status code: %s",
        r$status_code
      )
    }
    toRet <- FALSE
    attr(toRet, "m") <- m
  } else {
    toRet <- TRUE
  }
  return(toRet)
}

.check_db_session <- function(
  dbc,
  session_timeout = NA
) {
  r <- .send_query(
    dbc = dbc,
    query = "SELECT currentUser() as user",
    session_timeout = session_timeout
  )
  toRet <- .query_success(r)
  if (toRet) {
    attr(toRet, "user") <- sub("\n$", "", rawToChar(r$content))
  }
  return(toRet)
}
