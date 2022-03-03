###############################################################################@
## ClickHouseHTTPResult ----
#' ClickHouseHTTP result class.
#'
#' @export
#'
setClass(
   "ClickHouseHTTPResult",
   contains = "DBIResult",
   slots=list(
      sql="character",
      env="environment",
      conn="ClickHouseHTTPConnection",
      format="character"
   )
)

###############################################################################@
## dbFetch ----
#'
#' @rdname ClickHouseHTTPResult-class
#'
#' @export
#'
setMethod(
   "dbFetch", "ClickHouseHTTPResult",
   function(res, n=-1, ...){
      if(n!=-1){
         warning("Other values than -1 for n are not supported")
      }
      if(length(res@env$content)==0){
         toRet <- NULL
      }else{
         if(res@format=="Arrow"){
            toRet <- as.data.frame(.af_cast(
               arrow::read_feather(res@env$content, as_data_frame=FALSE),
               convert_uint=res@conn@convert_uint
            ))
         }
         if(res@format=="TabSeparatedWithNamesAndTypes"){
            l <- rawToChar(res@env$content)
            ctypes <- read.delim(
               text=l, header=TRUE, sep="\t", colClasses="character", nrows=1
            )
            toRet <- read.delim(
               text=l, header=FALSE, sep="\t", colClasses="character", skip=2
            )
            colnames(toRet) <- colnames(ctypes)
            attr(toRet, "types") <- ctypes
         }
      }
      res@env$fetched <- TRUE
      return(toRet)
   }
)

###############################################################################@
## dbClearResult ----
setMethod("dbClearResult", "ClickHouseHTTPResult", function(res, ...){
   res@env$content <- NULL
   res@env$fetched <- TRUE
   invisible(TRUE)
})

###############################################################################@
## dbHasCompleted ----
setMethod("dbHasCompleted", "ClickHouseHTTPResult", function(res, ...){
   !is.null(res@env$fetched) && resj@env$fetched
})

###############################################################################@
## dbIsValid ----
setMethod("dbIsValid", "ClickHouseHTTPResult", function(dbObj, ...){
   !is.null(dbObj@env$fetched) && !dbObj@env$fetched
})

###############################################################################@
## dbGetStatement ----
setMethod("dbGetStatement", "ClickHouseHTTPResult", function(res, ...){
   res@sql
})

###############################################################################@
## dbGetRowCount ----
setMethod("dbGetRowCount", "ClickHouseHTTPResult", function(res, ...){
   res@env$ch_summary$read_rows
})

###############################################################################@
## dbGetRowsAffected ----
setMethod("dbGetRowsAffected", "ClickHouseHTTPResult", function(res, ...){
   res@env$ch_summary$written_rows
})

###############################################################################@
## Helpers ----

.at_cast <- function(at, convert_uint=TRUE){
   if(inherits(at, "ListType")){
      return(arrow::list_of(.at_cast(at$value_type)))
   }
   toRet <- at
   if(inherits(at, "Binary")){
      toRet <- arrow::utf8()
   }
   if(convert_uint){
      if(inherits(at, "UInt8")){
         toRet <- arrow::boolean()
      }
      if(inherits(at, "UInt16")){
         toRet <- arrow::date32()
      }
      if(inherits(at, "Date32")){
         toRet <- arrow::int32()
      }
      if(inherits(at, "UInt32")){
         toRet <- arrow::timestamp()
      }
      if(inherits(at, "Timestamp")){
         toRet <- arrow::int64()
      }
   }
   return(toRet)
}
.sch_cast <- function(schema, convert_uint=TRUE){
   toRet <- list(do.call(
      arrow::schema,
      do.call(c, lapply(schema$fields, function(x){
         toRet <- list(.at_cast(x$type, convert_uint=convert_uint))
         names(toRet) <- x$name
         return(toRet)
      }))
   ))
   if(convert_uint){
      toRet <- c(
         list(do.call(
            arrow::schema,
            do.call(c, lapply(toRet[[1]]$fields, function(x){
               toRet <- list(.at_cast(x$type, convert_uint=convert_uint))
               names(toRet) <- x$name
               return(toRet)
            }))
         )),
         toRet
      )
   }
   return(toRet)
}
.af_cast <- function(af, convert_uint=TRUE){
   rs <- af$schema
   rsl <- .sch_cast(rs, convert_uint=convert_uint)
   for(i in 1:length(rsl)){
      af <- af$cast(rsl[[i]])
   }
   return(af)
}

