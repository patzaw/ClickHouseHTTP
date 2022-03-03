###############################################################################@
## ClickHouseHTTPConnection----
#' ClickHouseHTTP connection class.
#'
#' @export
#'
setClass(
   "ClickHouseHTTPConnection",
   contains = "DBIConnection",
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
#'
#' @rdname ClickHouseHTTPConnection-class
#'
#' @export
#'
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
#'
#' @rdname ClickHouseHTTPConnection-class
#'
#' @export
#'
setMethod("dbDisconnect", "ClickHouseHTTPConnection", function(conn, ...){
   xn <- deparse(substitute(conn))
   conn@session <- paste0(conn@session, "off")
   assign(xn, conn, envir=parent.frame(n=1))
   return(invisible(TRUE))
})

###############################################################################@
## dbSendQuery ----
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
#'
#' @rdname ClickHouseHTTPConnection-class
#'
#' @export
#'
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
   list(
      name="ClickHouseHTTPConnection",
      db.version=chinfo$version,
      uptime=chinfo$uptime,
      dbname=chinfo$database,
      username=dbObj@user,
      host=dbObj@host,
      port=dbObj@port,
      https=dbObj@https
   )

})

###############################################################################@
## dbListTables ----
#'
#' @rdname ClickHouseHTTPConnection-class
#'
#' @export
#'
setMethod("dbListTables", "ClickHouseHTTPConnection", function(conn, ...){
   as.character(DBI::dbGetQuery(conn, "SHOW TABLES")[[1]])
})

###############################################################################@
## dbDataType ----
#'
#' @rdname ClickHouseHTTPConnection-class
#'
#' @export
#'
setMethod(
   "dbDataType", "ClickHouseHTTPConnection", function(dbObj, obj, ...){
      dbDataType(ClickHouseHTTP(), obj)
   },
   valueClass = "character"
)

###############################################################################@
## dbQuoteIdentifier.character ----
#'
#' @rdname ClickHouseHTTPConnection-class
#'
#' @export
#'
setMethod(
   "dbQuoteIdentifier", c("ClickHouseHTTPConnection", "character"),
    function(conn, x, ...){
       if(anyNA(x)){
          stop("Input to dbQuoteIdentifier must not contain NA.")
       }else{
          x <- gsub('\\', '\\\\', x, fixed = TRUE)
          x <- gsub('`', '\\`', x, fixed = TRUE)
          DBI::SQL(paste0('`', x, '`'))
       }
    }
)

###############################################################################@
## dbQuoteIdentifier.SQL ----
#'
#' @rdname ClickHouseHTTPConnection-class
#'
#' @export
#'
setMethod(
   "dbQuoteIdentifier", c("ClickHouseHTTPConnection", "SQL"),
   function(conn, x, ...){x}
)

###############################################################################@
## dbQuoteString.character ----
#'
#' @rdname ClickHouseHTTPConnection-class
#'
#' @export
#'
setMethod(
   "dbQuoteString", c("ClickHouseHTTPConnection", "character"),
   function(conn, x, ...){
      x <- gsub('\\', '\\\\', x, fixed = TRUE)
      x <- gsub("'", "\\'", x, fixed = TRUE)
      return(DBI::SQL(ifelse(is.na(x), "NULL", paste0("'", x, "'"))))
   }
)

###############################################################################@
## dbQuoteString.SQL ----
#'
#' @rdname ClickHouseHTTPConnection-class
#'
#' @export
#'
setMethod(
   "dbQuoteString", c("ClickHouseHTTPConnection", "SQL"),
   function(conn, x, ...){x}
)

###############################################################################@
## dbExistsTable ----
#'
#' @rdname ClickHouseHTTPConnection-class
#'
#' @export
#'
setMethod(
   "dbExistsTable", c("ClickHouseHTTPConnection", "character"),
   function(conn, name, ...){
      qname <- dbQuoteIdentifier(conn, name)
      return(DBI::dbGetQuery(conn, paste("EXISTS", qname))[[1]])
      # as.logical(qname %in% dbQuoteIdentifier(conn, dbListTables(conn)))
   }
)

###############################################################################@
## dbReadTable ----
#'
#' @rdname ClickHouseHTTPConnection-class
#'
#' @export
#'
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
#'
#' @rdname ClickHouseHTTPConnection-class
#'
#' @export
#'
setMethod(
   "dbListFields", c("ClickHouseHTTPConnection", "character"),
   function(conn, name, ...){
   qname <- dbQuoteIdentifier(conn, name)
   DBI::dbGetQuery(conn, paste("DESCRIBE TABLE", qname))$name
})

###############################################################################@
## dbRemoveTable ----
#'
#' @rdname ClickHouseHTTPConnection-class
#'
#' @export
#'
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
#'
#' @rdname ClickHouseHTTPConnection-class
#'
#' @export
#'
setMethod(
   "dbAppendTable", "ClickHouseHTTPConnection",
   function(
      conn, name, value, ..., row.names = NULL
   ){
      qname <- dbQuoteIdentifier(conn, name)
      tmpf <- tempfile()
      on.exit(file.remove(tmpf))
      arrow::write_feather(value, tmpf)
      query <- sprintf("INSERT INTO %s FORMAT Arrow", qname)
      dbSendQuery(conn=conn, statement=query, file=tmpf)
      return(invisible(TRUE))
   }
)

###############################################################################@
## dbWriteTable ----
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
      engine="TinyLog", row.names=NULL, ...

   ){
      if(overwrite && append){
         stop("overwrite and append cannot be both TRUE")
      }
      qname <- dbQuoteIdentifier(conn, name)
      if(!append){
         cdef <- unlist(lapply(
            value, function(x){
               type <- dbDataType(k, x)
               if(any(is.na(x))){
                  type <- sprintf("Nullable(%s)", type)
               }
               return(type)
            }
         ))
         fields <- paste(names(cdef), cdef)
         dbCreateTable(
            conn=conn, name=name, fields=fields,
            engine=engine, overwrite=overwrite
         )
      }
      dbAppendTable(conn=conn, name=name, value=value)
      return(invisible(TRUE))
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
