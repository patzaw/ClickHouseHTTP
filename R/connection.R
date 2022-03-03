###############################################################################@
## ClickHouseHTTPConnection----
#' ClickHouseHTTPConnection class.
#'
#' @export
#'
setClass(
   "ClickHouseHTTPConnection",
   contains="DBIConnection",
   slots=list(
      host="character",
      port="integer",
      user="character",
      password="function",
      https="logical",
      ssl_verifypeer="logical",
      session="character",
      convert_uint="logical"
   )
)

###############################################################################@
## dbIsValid ----
##
setMethod("dbIsValid", "ClickHouseHTTPConnection", function(dbObj, ...){
   toRet <- try(.check_db_session(dbObj), silent=TRUE)
   if(inherits(toRet, "try-error")){
      v <- FALSE
      attr(v, "m") <- as.character(toRet)
      toRet <- v
   }
   if(!toRet){
      warning(attr(toRet, "m"))
      toRet <- as.logical(toRet)
   }
   return(toRet)
})

###############################################################################@
## dbDisconnect ----
##
setMethod("dbDisconnect", "ClickHouseHTTPConnection", function(conn, ...){
   xn <- deparse(substitute(conn))
   conn@session <- paste0(conn@session, "off")
   assign(xn, conn, envir=parent.frame(n=1))
   return(invisible(TRUE))
})

###############################################################################@
## dbSendQuery ----
#' Send SQL query to ClickHouse
#'
#' @param conn a ClickHouseHTTPConnection object created with [dbConnect()]
#' @param statement the SQL query statement
#' @param format the format used by ClickHouse to send the results.
#' Two formats are supported:
#' "Arrow" (default) and "TabSeparatedWithNamesAndTypes"
#' @param file a path to a file to send along the query (default: NA)
#' @param ... Other parameters passed on to methods
#'
#' @return A ClickHouseHTTPResult object
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
      conn, statement,
      format=c("Arrow", "TabSeparatedWithNamesAndTypes"),
      file=NA,
      ...
   ){
      format <- match.arg(format)
      resEnv <- new.env(parent=emptyenv())
      query <- statement
      if(length(grep(
         "^[[:space:]]*(SELECT|DESC|DESCRIBE|SHOW|WITH|EXISTS)[[:space:]]+",
         statement, value=T, ignore.case=TRUE
      ))==1){
         query=paste(query, "FORMAT", format)
      }
      r <- .send_query(
         dbc=conn, query=query, file=file
      )
      v <- .query_success(r)
      if(!v){
         m <- attr(v, "m")
         if(length(grep("Arrow data format", m)) > 0){
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
         sql=statement,
         env=resEnv,
         conn=conn,
         format=format
      ))
   }
)
###############################################################################@
## show ----
##
setMethod("show", "ClickHouseHTTPConnection", function(object){
   cat("<ClickHouseHTTPConnection>\n")
   if(dbIsValid(object)){
      cat(sprintf(
         "   %s://%s@%s:%s\n",
         ifelse(object@https, "https", "http"),
         object@user, object@host, object@port
      ))
   }else{
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
setMethod("dbGetInfo", "ClickHouseHTTPConnection", function(dbObj, ...){
   chinfo <- DBI::dbGetQuery(
      dbObj,
      paste(
         "SELECT version() as version, uptime() as uptime",
         ", currentDatabase() as database"
      )
   )
   return(list(
      name="ClickHouseHTTPConnection",
      db.version=chinfo$version,
      uptime=chinfo$uptime,
      dbname=chinfo$database,
      username=dbObj@user,
      host=dbObj@host,
      port=dbObj@port,
      https=dbObj@https
   ))

})

###############################################################################@
## dbListTables ----
##
setMethod("dbListTables", "ClickHouseHTTPConnection", function(conn, ...){
   return(as.character(DBI::dbGetQuery(conn, "SHOW TABLES")[[1]]))
})

###############################################################################@
## dbDataType ----
##
setMethod(
   "dbDataType", "ClickHouseHTTPConnection", function(dbObj, obj, ...){
      return(dbDataType(ClickHouseHTTP(), obj))
   },
   valueClass="character"
)

###############################################################################@
## dbQuoteIdentifier.character ----
##
setMethod(
   "dbQuoteIdentifier", c("ClickHouseHTTPConnection", "character"),
    function(conn, x, ...){
       if(anyNA(x)){
          stop("Input to dbQuoteIdentifier must not contain NA.")
       }else{
          x <- gsub('\\', '\\\\', x, fixed=TRUE)
          x <- gsub('`', '\\`', x, fixed=TRUE)
          return(DBI::SQL(paste0('`', x, '`')))
       }
    }
)

###############################################################################@
## dbQuoteIdentifier.SQL ----
##
setMethod(
   "dbQuoteIdentifier", c("ClickHouseHTTPConnection", "SQL"),
   function(conn, x, ...){
      return(x)
   }
)

###############################################################################@
## dbQuoteString.character ----
##
setMethod(
   "dbQuoteString", c("ClickHouseHTTPConnection", "character"),
   function(conn, x, ...){
      x <- gsub('\\', '\\\\', x, fixed=TRUE)
      x <- gsub("'", "\\'", x, fixed=TRUE)
      return(DBI::SQL(ifelse(is.na(x), "NULL", paste0("'", x, "'"))))
   }
)

###############################################################################@
## dbQuoteString.SQL ----
##
setMethod(
   "dbQuoteString", c("ClickHouseHTTPConnection", "SQL"),
   function(conn, x, ...){
      return(x)
   }
)

###############################################################################@
## dbExistsTable ----
##
setMethod(
   "dbExistsTable", c("ClickHouseHTTPConnection", "character"),
   function(conn, name, ...){
      qname <- dbQuoteIdentifier(conn, name)
      return(DBI::dbGetQuery(conn, paste("EXISTS", qname))[[1]])
   }
)

###############################################################################@
## dbReadTable ----
##
setMethod(
   "dbReadTable", c("ClickHouseHTTPConnection", "character"),
   function(conn, name,...){
      qname <- dbQuoteIdentifier(conn, name)
      ## Identify UUID columns for converting them in String
      coltype <- DBI::dbGetQuery(
         conn,
         paste("DESCRIBE TABLE", qname)
      )[, c("name", "type")]
      if(any(coltype$type=="UUID")){
         query <- paste(
            "SELECT",
            paste(ifelse(
               coltype$type=="UUID",
               sprintf(
                  "toString(%s) as %s",
                  dbQuoteIdentifier(conn, coltype$name),
                  dbQuoteIdentifier(conn, coltype$name)
               ),
               dbQuoteIdentifier(conn, coltype$name)
            ), collapse=", ")
         )
      }else{
         query <- "SELECT *"
      }
      return(DBI::dbGetQuery(
         conn,
         paste(query, "FROM", qname),
         ...
      ))
   }
)

###############################################################################@
## dbListFields ----
##
setMethod(
   "dbListFields", c("ClickHouseHTTPConnection", "character"),
   function(conn, name, ...){
   qname <- dbQuoteIdentifier(conn, name)
   return(DBI::dbGetQuery(conn, paste("DESCRIBE TABLE", qname))$name)
})

###############################################################################@
## dbRemoveTable ----
##
setMethod(
   "dbRemoveTable", "ClickHouseHTTPConnection",
   function(conn, name, ...){
      qname <- dbQuoteIdentifier(conn, name)
      dbSendQuery(conn, paste0("DROP TABLE ", qname))
      return(invisible(TRUE))
   }
)

###############################################################################@
## dbCreateTable ----
#' Create a table in ClickHouse
#'
#' @param conn a ClickHouseHTTPConnection object created with [dbConnect()]
#' @param name the name of the table to create
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
#' @param ... Other parameters passed on to methods
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
   "dbCreateTable", "ClickHouseHTTPConnection",
   function(
      conn, name, fields, engine="TinyLog",
      overwrite=FALSE, ..., row.names=NULL, temporary=FALSE
   ){
      qname <- dbQuoteIdentifier(conn, name)
      if(overwrite && dbExistsTable(conn, qname)){
         dbRemoveTable(conn, qname)
      }

      query <- paste(
         sprintf(
            "CREATE TABLE %s (",
            qname
         ),
         paste(fields, collapse=",\n"),
         ") ENGINE =", engine
      )

      dbSendQuery(conn, query)
      return(invisible(TRUE))
   }
)

###############################################################################@
## dbAppendTable ----
##
setMethod(
   "dbAppendTable", "ClickHouseHTTPConnection",
   function(
      conn, name, value, ..., row.names=NULL
   ){
      qname <- dbQuoteIdentifier(conn, name)
      tmpf <- tempfile()
      on.exit(file.remove(tmpf))
      arrow::write_feather(value, tmpf)
      query <- sprintf("INSERT INTO %s FORMAT Arrow", qname)
      res <- dbSendQuery(conn=conn, statement=query, file=tmpf)
      return(invisible(dbGetRowsAffected(res)))
   }
)

###############################################################################@
## dbWriteTable ----
#' Write a table in ClickHouse
#'
#' @param conn a ClickHouseHTTPConnection object created with [dbConnect()]
#' @param name the name of the table to create
#' @param value the table to write
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
   "dbWriteTable", "ClickHouseHTTPConnection",
   function(
      conn, name, value,
      overwrite=FALSE, append=FALSE,
      engine="TinyLog", ...

   ){
      if(overwrite && append){
         stop("overwrite and append cannot be both TRUE")
      }
      qname <- dbQuoteIdentifier(conn, name)
      if(!append){
         cdef <- unlist(lapply(
            value, function(x){
               type <- dbDataType(conn, x)
               if(any(is.na(x))){
                  type <- sprintf("Nullable(%s)", type)
               }
               return(type)
            }
         ))
         fields <- paste(dbQuoteIdentifier(conn, names(cdef)), cdef)
         dbCreateTable(
            conn=conn, name=name, fields=fields,
            engine=engine, overwrite=overwrite
         )
      }
      toRet <- dbAppendTable(conn=conn, name=name, value=value)
      return(invisible(toRet))
   }
)

###############################################################################@
## Helpers ----

.build_http_req <- function(
   host, port, https,
   session, session_timeout=NA,
   query=""
){
   sprintf(
      "%s://%s:%s/?session_id=%s&session_check=%s%s&query=%s",
      ifelse(https, "https", "http"),
      host, port, session, as.integer(is.na(session_timeout)),
      ifelse(
         is.na(session_timeout), "",
         sprintf("&session_timeout=%s", as.integer(session_timeout))
      ),
      utils::URLencode(query)
   )
}

.send_query <- function(
   dbc,
   query,
   file=NA,
   session_timeout=NA
){
   if(is.na(file)){
      qbody <- query
      query <- ""
   }else{
      qbody <- httr::upload_file(file)
      query <- utils::URLencode(query)
   }
   httr::POST(
      url=.build_http_req(
         host=dbc@host, port=dbc@port, https=dbc@https,
         session=dbc@session, session_timeout=session_timeout,
         query=query
      ),
      body=qbody,
      add_headers(
         'X-ClickHouse-User'=dbc@user,
         'X-ClickHouse-Key'=dbc@password()
      ),
      config=config(ssl_verifypeer=as.integer(dbc@ssl_verifypeer))
   )
}

.query_success <- function(r){
   if(
      r$status_code >= 300 ||
      !is.null(r$headers$`x-clickhouse-exception-code`)
   ){
      if(!is.null(r$headers$`x-clickhouse-exception-code`)){
         m <- rawToChar(r$content)
      }else{
         m <- sprintf(
            "Connection error. Status code: %s",
            r$status_code
         )
      }
      toRet <- FALSE
      attr(toRet, "m") <- m
   }else{
      toRet <- TRUE
   }
   return(toRet)
}

.check_db_session <- function(
   dbc, session_timeout=NA
){
   r <- .send_query(dbc=dbc, query="SELECT 1", session_timeout=session_timeout)
   return(.query_success(r))
}
