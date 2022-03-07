###############################################################################@
## ClickHouseHTTPResult ----
#' ClickHouseHTTPResult class.
#'
#' @export
#'
setClass(
   "ClickHouseHTTPResult",
   contains="DBIResult",
   slots=list(
      sql="character",
      env="environment",
      conn="ClickHouseHTTPConnection",
      format="character"
   )
)

###############################################################################@
## dbFetch ----
##
setMethod(
   "dbFetch", "ClickHouseHTTPResult",
   function(res, n=-1, ...){
      if(n!=-1){
         warning("Other values than -1 for n are not supported")
      }
      if(length(res@env$content)==0){
         toRet <- data.frame()
      }else{
         if(res@format=="Arrow"){
            toRet <- as.data.frame(.af_cast(
               arrow::read_feather(res@env$content, as_data_frame=FALSE),
               convert_uint=res@conn@convert_uint
            ))
         }
         if(res@format=="TabSeparatedWithNamesAndTypes"){

            l <- try(rawToChar(res@env$content), silent=TRUE)
            if(inherits(l, "try-error")){
               tmpf <- tempfile()
               on.exit(file.remove(tmpf))
               writeBin(res@env$content, con=tmpf)
            }else{
               tmpf <- NA
            }
            if(is.na(tmpf)){
               ctypes <- data.table::fread(
                  text=l,
                  header=TRUE, sep="\t", colClasses="character", nrows=1,
                  stringsAsFactors=FALSE
               )
            }else{
               ctypes <- data.table::fread(
                  file=tmpf,
                  header=TRUE, sep="\t", colClasses="character", nrows=1,
                  stringsAsFactors=FALSE
               )
            }
            chClasses <- as.character(t(ctypes))
            chType <- sub("^.*[(]", "", sub("[)].*$", "", chClasses))
            chArray <- grepl("Array[(].*[)]", chClasses)
            rType <-
               ifelse(
                  grepl("DateTime", chType), "POSIXct",
               ifelse(
                  grepl("Date", chType), "Date",
               ifelse(
                  grepl("Float", chType), "numeric",
               ifelse(
                  grepl("Decimal", chType), "numeric",
               ifelse(
                  chType=="UInt8" & res@conn@convert_uint, "logical",
               ifelse(
                  grepl("Int64", chType), "integer64",
               ifelse(
                  grepl("String", chType), "character",
               ifelse(
                  grepl("UUID", chType), "character",
               ifelse(
                  grepl("Int", chType), "integer",
                  NA
               )))))))))
            if(any(is.na(rType))){
               ut <- unique(chType[which(is.na(rType))])
               warning(sprintf(
                  'Unsupported type(s): %s --> "character"', paste(ut, sep=", ")
               ))
               rType <- ifelse(is.na(rType), "character", rType)
            }
            if(is.na(tmpf)){
               toRet <- try(data.table::fread(
                  text=l,
                  header=FALSE, sep="\t",
                  colClasses=ifelse(chArray, "character", rType), skip=2,
                  stringsAsFactors=FALSE, na.strings="\\N",
                  logical01=TRUE
               ), silent=TRUE)
               if(inherits(toRet, "try-error")){
                  if(length(grep("skip=2 but the input only has", toRet)) > 0){
                     toRet <- try(data.table::fread(
                        text=l,
                        header=FALSE, sep="\t",
                        colClasses=ifelse(chArray, "character", rType),
                        nrow=0,
                        stringsAsFactors=FALSE, na.strings="\\N",
                        logical01=TRUE
                     ), silent=TRUE)
                  }else{
                     stop(as.character(toRet))
                  }
               }
            }else{
               toRet <- try(data.table::fread(
                  file=tmpf,
                  header=FALSE, sep="\t",
                  colClasses=ifelse(chArray, "character", rType), skip=2,
                  stringsAsFactors=FALSE, na.strings="\\N",
                  logical01=TRUE
               ), silent=TRUE)
               if(inherits(toRet, "try-error")){
                  if(length(grep("skip=2 but the input only has", toRet)) > 0){
                     toRet <- try(data.table::fread(
                        file=tmpf,
                        header=FALSE, sep="\t",
                        colClasses=ifelse(chArray, "character", rType),
                        nrow=0,
                        stringsAsFactors=FALSE, na.strings="\\N",
                        logical01=TRUE
                     ), silent=TRUE)
                  }else{
                     stop(as.character(toRet))
                  }
               }
            }
            for(i in which(chArray)){
               toRet[[i]] <- .split_txt_array(toRet[[i]], type=rType[i])
            }
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
##
setMethod("dbClearResult", "ClickHouseHTTPResult", function(res, ...){
   res@env$content <- NULL
   res@env$fetched <- TRUE
   invisible(TRUE)
})

###############################################################################@
## dbHasCompleted ----
##
setMethod("dbHasCompleted", "ClickHouseHTTPResult", function(res, ...){
   !is.null(res@env$fetched) && res@env$fetched
})

###############################################################################@
## dbIsValid ----
##
setMethod("dbIsValid", "ClickHouseHTTPResult", function(dbObj, ...){
   !is.null(dbObj@env$fetched) && !dbObj@env$fetched
})

###############################################################################@
## dbGetStatement ----
##
setMethod("dbGetStatement", "ClickHouseHTTPResult", function(res, ...){
   res@sql
})

###############################################################################@
## dbGetRowCount ----
##
setMethod("dbGetRowCount", "ClickHouseHTTPResult", function(res, ...){
   res@env$ch_summary$read_rows
})

###############################################################################@
## dbGetRowsAffected ----
##
setMethod("dbGetRowsAffected", "ClickHouseHTTPResult", function(res, ...){
   res@env$ch_summary$written_rows
})

###############################################################################@
## Helpers ----

### Arrow cast ----
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

### Array from text ----
.split_txt_array <- function(x, type){
   y <- gsub("(^[[]|[]]$)", "", x)
   y <- strsplit(y, split=ifelse(type=="character", "','", ","))
   y <- lapply(y, function(z) sub("(^'|'$)", "", z))
   if(type=="Date"){
      y <- lapply(y, as.Date)
   }
   if(type=="POSIXct"){
      y <- lapply(y, as.POSIXct)
   }
   if(!type %in% c("character", "Date", "POSIXct")){
      y <- lapply(y, as, class=type)
   }
   return(y)
}
