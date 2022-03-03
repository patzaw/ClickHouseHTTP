###############################################################################@
## ClickHouseHTTPDriver ----
#' Driver for the ClickHouse database using HTTP(S) interface
#'
#' @export
#'
setClass("ClickHouseHTTPDriver", contains = "DBIDriver")

###############################################################################@
## dbUnloadDrive ----
##
setMethod("dbUnloadDriver", "ClickHouseHTTPDriver", function(drv, ...){
   TRUE
})

###############################################################################@
## show ----
##
setMethod("show", "ClickHouseHTTPDriver", function(object){
   cat("<ClickHouseHTTPDriver>\n")
})

###############################################################################@
## Driver creator ----
#' Create a ClickHouseHTTP DBI driver
#'
#' @return A ClickHouseHTTPDriver
#'
#' @seealso [ClickHouseHTTPDriver-class]
#'
#' @export
#'
ClickHouseHTTP <- function(){
   new("ClickHouseHTTPDriver")
}

###############################################################################@
## dbConnect ----
#' Connect to a ClickHouse database using the ClickHouseHTTP DBI
#'
#' @param drv A driver object created by [ClickHouseHTTP()]
#' @param host name of the database host (default: "localhost")
#' @param port port on which the database is listening (default: 8123L)
#' @param dbname name of the default database (default: "default")
#' @param user user name (default: "default")
#' @param password user password (default: "")
#' @param https a logical to use the HTTPS protocol (default: FALSE)
#' @param ssl_verifypeer a logical to verify SSL certificate when using
#' HTTPS (default: TRUE)
#' @param session_timeout timeout in seconds (default: 3600L seconds)
#' @param convert_uint a logical: if TRUE (default), UInt ClickHouse
#' data types are converted in the following R classes:
#' - UInt8 : logical
#' - UInt16: Date
#' - UInt32: POSIXct
#' @param ... Other parameters passed on to methods
#'
#' @return A ClickHouseHTTPConnection
#'
#' @example supp/examples/global-example.R
#'
#' @rdname ClickHouseHTTPDriver-class
#'
#' @seealso [ClickHouseHTTPConnection-class]
#'
#' @export
#'
setMethod(
   "dbConnect", "ClickHouseHTTPDriver",
   function(
      drv,
      host="localhost",
      port=8123L,
      dbname="default",
      user="default",
      password="",
      https=FALSE,
      ssl_verifypeer=TRUE,
      session_timeout=3600L,
      convert_uint=TRUE,
      ...
   ){
      stopifnot(
         is.character(host), length(host)==1, !is.na(host),
         is.numeric(port), length(port)==1, !is.na(port),
         as.integer(port)==port,
         is.character(dbname), length(dbname)==1, !is.na(dbname),
         is.character(user), length(user)==1, !is.na(user),
         is.character(password), length(password)==1, !is.na(password),
         is.logical(https), length(https)==1, !is.na(https),
         is.logical(ssl_verifypeer), length(ssl_verifypeer)==1,
         !is.na(ssl_verifypeer),
         is.numeric(session_timeout), length(session_timeout)==1,
         !is.na(session_timeout),
         is.logical(convert_uint), length(convert_uint)==1,
         !is.na(convert_uint)
      )
      session <- paste(
         format(Sys.time(), format="%Y%m%d%H%M%S"),
         paste(sample(c(letters, LETTERS), 10), collapse=""),
         sep=""
      )
      toRet <- new(
         "ClickHouseHTTPConnection",
         host=host,
         port=as.integer(port),
         user=user,
         password=do.call(
            function(p){return(function() invisible(p))},list(p=password)
         ),
         https=https,
         ssl_verifypeer=ssl_verifypeer,
         session=session,
         convert_uint=convert_uint
      )

      ## Check connection
      v <- .check_db_session(toRet, session_timeout=session_timeout)
      if(!v){
         stop(attr(v, "m"))
      }

      ## Set default db
      dbSendQuery(toRet, sprintf("USE `%s`", dbname))
      dbSendQuery(toRet, "SET send_progress_in_http_headers=1")

      return(toRet)
   }
)

###############################################################################@
## dbDataType ----
##
setMethod(
   "dbDataType", "ClickHouseHTTPDriver",
   function(dbObj, obj, ...){
      toRet <-
         ifelse(
            is.list(obj),
            sprintf("Array(%s)", dbDataType(dbObj, unlist(obj, recursive=F))),
         ifelse(
            is.logical(obj), "UInt8",
         ifelse(
            is.integer(obj), "Int32",
         ifelse(
            is.numeric(obj), "Float64",
         ifelse(
            inherits(obj, "POSIXct"), "DateTime",
         ifelse(
            inherits(obj, "Date"), "Date",
         ifelse(
            is.character(obj), "String",
            as.character(NA)
         )))))))
      if(is.na(toRet)){
         warning(sprintf(
            "Unsupported object class: %s",
            paste(class(obj), collapse=", ")
         ))
      }
      return(toRet)
   },
   valueClass="character"
)

