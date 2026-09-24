###############################################################################@
## ClickHouseHTTPResult ----
#' ClickHouseHTTPResult class.
#'
#' @export
#'
setClass(
  "ClickHouseHTTPResult",
  contains = "DBIResult",
  slots = list(
    sql = "character",
    env = "environment",
    conn = "ClickHouseHTTPConnection",
    format = "character"
  )
)

###############################################################################@
## dbFetch ----
##
setMethod(
  "dbFetch",
  "ClickHouseHTTPResult",
  function(res, n = -1, ...) {
    if (n != -1) {
      warning("Other values than -1 for n are not supported")
    }
    if (length(res@env$content) == 0) {
      toRet <- data.frame()
    } else {
      if (res@format == "Arrow") {
        toRet <- as.data.frame(.af_cast(
          arrow::read_feather(res@env$content, as_data_frame = FALSE),
          convert_uint = res@conn@convert_uint,
          session_timezone = .session_timezone(res@conn@settings)
        ))
      }
      if (res@format == "TabSeparatedWithNamesAndTypes") {
        l <- try(rawToChar(res@env$content), silent = TRUE)
        if (inherits(l, "try-error")) {
          tmpf <- tempfile()
          on.exit(file.remove(tmpf))
          writeBin(res@env$content, con = tmpf)
        } else {
          tmpf <- NA
        }
        if (is.na(tmpf)) {
          ctypes <- data.table::fread(
            text = l,
            header = TRUE,
            sep = "\t",
            colClasses = "character",
            nrows = 1,
            stringsAsFactors = FALSE,
            quote = ""
          )
        } else {
          ctypes <- data.table::fread(
            file = tmpf,
            header = TRUE,
            sep = "\t",
            colClasses = "character",
            nrows = 1,
            stringsAsFactors = FALSE,
            quote = ""
          )
        }
        chClasses <- as.character(t(ctypes))
        chType <- ifelse(
          grepl("DateTime", chClasses),
          "DateTime",
          sub("^.*[(]", "", sub("[)].*$", "", chClasses))
        )
        chArray <- grepl("Array[(].*[)]", chClasses)
        rType <-
          ifelse(
            grepl("DateTime", chType),
            "POSIXct",
            ifelse(
              grepl("Date", chType),
              "Date",
              ifelse(
                grepl("Float", chType),
                "numeric",
                ifelse(
                  grepl("Decimal", chType),
                  "numeric",
                  ifelse(
                    chType == "UInt8" & res@conn@convert_uint,
                    "logical",
                    ifelse(
                      grepl("Int64", chType),
                      "integer64",
                      ifelse(
                        grepl("String", chType),
                        "character",
                        ifelse(
                          grepl("UUID", chType),
                          "character",
                          ifelse(
                            grepl("Int", chType),
                            "integer",
                            NA
                          )
                        )
                      )
                    )
                  )
                )
              )
            )
          )
        session_timezone <- .session_timezone(res@conn@settings)
        col_tz <- .type_timezone(chClasses)
        effective_tz <- ifelse(
          !is.na(col_tz),
          col_tz,
          ifelse(is.null(session_timezone), NA_character_, session_timezone)
        )
        cast_type <- function(type, x, tz = NA_character_) {
          switch(
            type,
            "integer" = as.integer(x),
            "numeric" = as.numeric(x),
            "logical" = as.logical(x),
            "character" = as.character(x),
            "Date" = as.Date(x),
            "POSIXct" = if (is.na(tz)) {
              y <- as.POSIXct(x, tz = "UTC")
              attr(y, "tzone") <- NULL
              y
            } else {
              as.POSIXct(x, tz = tz)
            },
            "integer64" = as(x, "integer64")
          )
        }
        fread_type <- ifelse(rType == "POSIXct", "character", rType)
        if (any(is.na(rType))) {
          ut <- unique(chType[which(is.na(rType))])
          warning(sprintf(
            'Unsupported type(s): %s --> "character"',
            paste(ut, sep = ", ")
          ))
          rType <- ifelse(is.na(rType), "character", rType)
        }
        if (is.na(tmpf)) {
          toRet <- try(
            data.table::fread(
              text = l,
              header = FALSE,
              sep = "\t",
              colClasses = ifelse(chArray, "character", fread_type),
              skip = 2,
              stringsAsFactors = FALSE,
              na.strings = "\\N",
              logical01 = TRUE,
              quote = ""
            ),
            silent = TRUE
          )
          if (inherits(toRet, "try-error")) {
            if (length(grep("skip=2 but the input only has", toRet)) > 0) {
              toRet <- try(
                data.table::fread(
                  text = l,
                  header = FALSE,
                  sep = "\t",
                  # colClasses=ifelse(chArray, "character", rType),
                  nrow = 0,
                  stringsAsFactors = FALSE,
                  na.strings = "\\N",
                  logical01 = TRUE,
                  quote = ""
                ),
                silent = TRUE
              )
              for (i in seq_len(ncol(toRet))) {
                toRet[[i]] <- cast_type(
                  rType[i],
                  toRet[[i]],
                  tz = effective_tz[i]
                )
              }
            } else {
              stop(as.character(toRet))
            }
          }
        } else {
          toRet <- try(
            data.table::fread(
              file = tmpf,
              header = FALSE,
              sep = "\t",
              colClasses = ifelse(chArray, "character", fread_type),
              skip = 2,
              stringsAsFactors = FALSE,
              na.strings = "\\N",
              logical01 = TRUE,
              quote = ""
            ),
            silent = TRUE
          )
          if (inherits(toRet, "try-error")) {
            if (length(grep("skip=2 but the input only has", toRet)) > 0) {
              toRet <- try(
                data.table::fread(
                  file = tmpf,
                  header = FALSE,
                  sep = "\t",
                  # colClasses=ifelse(chArray, "character", rType),
                  nrow = 0,
                  stringsAsFactors = FALSE,
                  na.strings = "\\N",
                  logical01 = TRUE,
                  quote = ""
                ),
                silent = TRUE
              )
              for (i in seq_len(ncol(toRet))) {
                toRet[[i]] <- cast_type(
                  rType[i],
                  toRet[[i]],
                  tz = effective_tz[i]
                )
              }
            } else {
              stop(as.character(toRet))
            }
          }
        }
        for (i in which(rType == "POSIXct" & !chArray)) {
          toRet[[i]] <- cast_type(rType[i], toRet[[i]], tz = effective_tz[i])
        }
        for (i in which(chArray)) {
          toRet[[i]] <- .split_txt_array(
            toRet[[i]],
            type = rType[i],
            tz = effective_tz[i]
          )
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
setMethod("dbClearResult", "ClickHouseHTTPResult", function(res, ...) {
  res@env$content <- NULL
  res@env$fetched <- TRUE
  invisible(TRUE)
})

###############################################################################@
## dbHasCompleted ----
##
setMethod("dbHasCompleted", "ClickHouseHTTPResult", function(res, ...) {
  !is.null(res@env$fetched) && res@env$fetched
})

###############################################################################@
## dbIsValid ----
##
setMethod("dbIsValid", "ClickHouseHTTPResult", function(dbObj, ...) {
  !is.null(dbObj@env$fetched) && !dbObj@env$fetched
})

###############################################################################@
## dbGetStatement ----
##
setMethod("dbGetStatement", "ClickHouseHTTPResult", function(res, ...) {
  res@sql
})

###############################################################################@
## dbGetRowCount ----
##
setMethod("dbGetRowCount", "ClickHouseHTTPResult", function(res, ...) {
  res@env$ch_summary$read_rows
})

###############################################################################@
## dbGetRowsAffected ----
##
setMethod(
  "dbGetRowsAffected",
  "ClickHouseHTTPResult",
  function(res, ...) {
    res@env$ch_summary$written_rows
  }
)

###############################################################################@
## dbColumnInfo ----
##
setMethod(
  "dbColumnInfo",
  "ClickHouseHTTPResult",
  function(res, ...) {
    if (length(res@env$content) == 0) {
      return(data.frame(
        name = character(),
        type = character(),
        stringsAsFactors = FALSE
      ))
    }
    if (res@format == "Arrow") {
      af <- arrow::read_feather(res@env$content, as_data_frame = FALSE)
      rs <- af$schema
      rsl <- .sch_cast(
        rs,
        convert_uint = res@conn@convert_uint,
        session_timezone = .session_timezone(res@conn@settings)
      )
      final_schema <- rsl[[length(rsl)]]
      col_names <- sapply(final_schema$fields, function(x) x$name)
      col_types <- sapply(final_schema$fields, function(x) {
        .arrow_type_to_r(x$type)
      })
      return(data.frame(
        name = col_names,
        type = col_types,
        stringsAsFactors = FALSE
      ))
    }
    if (res@format == "TabSeparatedWithNamesAndTypes") {
      l <- try(rawToChar(res@env$content), silent = TRUE)
      if (inherits(l, "try-error")) {
        tmpf <- tempfile()
        on.exit(file.remove(tmpf))
        writeBin(res@env$content, con = tmpf)
        ctypes <- data.table::fread(
          file = tmpf,
          header = TRUE,
          sep = "\t",
          colClasses = "character",
          nrows = 1,
          stringsAsFactors = FALSE,
          quote = ""
        )
      } else {
        ctypes <- data.table::fread(
          text = l,
          header = TRUE,
          sep = "\t",
          colClasses = "character",
          nrows = 1,
          stringsAsFactors = FALSE,
          quote = ""
        )
      }
      chClasses <- as.character(t(ctypes))
      chType <- ifelse(
        grepl("DateTime", chClasses),
        "DateTime",
        sub("^.*[(]", "", sub("[)].*$", "", chClasses))
      )
      rType <- ifelse(
        grepl("DateTime", chType),
        "POSIXct",
        ifelse(
          grepl("Date", chType),
          "Date",
          ifelse(
            grepl("Float", chType),
            "numeric",
            ifelse(
              grepl("Decimal", chType),
              "numeric",
              ifelse(
                chType == "UInt8" & res@conn@convert_uint,
                "logical",
                ifelse(
                  grepl("Int64", chType),
                  "integer64",
                  ifelse(
                    grepl("String", chType),
                    "character",
                    ifelse(
                      grepl("UUID", chType),
                      "character",
                      ifelse(grepl("Int", chType), "integer", "character")
                    )
                  )
                )
              )
            )
          )
        )
      )
      return(data.frame(
        name = colnames(ctypes),
        type = rType,
        stringsAsFactors = FALSE
      ))
    }
  }
)

###############################################################################@
## Helpers ----

### Arrow type to R type ----
.arrow_type_to_r <- function(type) {
  if (inherits(type, "ListType")) {
    return("list")
  }
  if (inherits(type, "Boolean")) {
    return("logical")
  }
  if (inherits(type, c("Date32", "Date64"))) {
    return("Date")
  }
  if (inherits(type, "Timestamp")) {
    return("POSIXct")
  }
  if (
    inherits(type, c("Int8", "Int16", "Int32", "UInt8", "UInt16", "UInt32"))
  ) {
    return("integer")
  }
  if (inherits(type, c("Int64", "UInt64"))) {
    return("integer64")
  }
  if (inherits(type, c("Float32", "Float64", "Decimal128", "Decimal256"))) {
    return("numeric")
  }
  if (inherits(type, c("Utf8", "LargeUtf8", "Binary", "LargeBinary"))) {
    return("character")
  }
  return("character")
}

### Arrow cast ----
.session_timezone <- function(settings) {
  if (length(settings) != 1 || is.na(settings) || !nzchar(settings)) {
    return(NULL)
  }
  settings <- strsplit(settings, "&", fixed = TRUE)[[1]]
  setting <- settings[startsWith(settings, "session_timezone=")][1]
  if (is.na(setting)) {
    return(NULL)
  }
  sub("^session_timezone=", "", setting)
}
.at_cast <- function(at, convert_uint = TRUE, session_timezone = NULL) {
  if (inherits(at, "ListType")) {
    return(arrow::list_of(.at_cast(
      at$value_type,
      session_timezone = session_timezone
    )))
  }
  toRet <- at
  if (inherits(at, "Binary")) {
    toRet <- arrow::utf8()
  }
  if (convert_uint) {
    if (inherits(at, "UInt8")) {
      toRet <- arrow::boolean()
    }
    if (inherits(at, "UInt16")) {
      toRet <- arrow::date32()
    }
    if (inherits(at, "UInt32")) {
      toRet <- if (is.null(session_timezone)) {
        arrow::timestamp()
      } else {
        arrow::timestamp(timezone = session_timezone)
      }
    }
  }
  return(toRet)
}
.at_intermediate <- function(at) {
  ## For UInt16/UInt32 sent by older ClickHouse, Arrow cannot cast directly to
  ## date32/timestamp. We need an intermediate int32/int64 cast first.
  ## This function returns the intermediate type for that first step.
  if (inherits(at, "ListType")) {
    inner <- .at_intermediate(at$value_type)
    if (is.null(inner)) {
      return(NULL)
    }
    return(arrow::list_of(inner))
  }
  if (inherits(at, "UInt16")) {
    return(arrow::int32())
  }
  if (inherits(at, "UInt32")) {
    return(arrow::int64())
  }
  return(NULL)
}
.sch_cast <- function(
  schema,
  convert_uint = TRUE,
  session_timezone = NULL
) {
  field_names <- sapply(schema$fields, function(x) x$name)
  orig_types <- lapply(schema$fields, function(x) x$type)

  make_schema <- function(types) {
    do.call(
      arrow::schema,
      stats::setNames(types, field_names)
    )
  }

  final_types <- lapply(orig_types, function(t) {
    .at_cast(
      t,
      convert_uint = convert_uint,
      session_timezone = session_timezone
    )
  })

  if (!convert_uint) {
    return(list(make_schema(final_types)))
  }

  ## For columns where ClickHouse sends UInt16/UInt32 (old-style Date/DateTime),
  ## Arrow requires an intermediate int32/int64 cast before casting to
  ## date32/timestamp.
  ## Columns already typed as date32/timestamp pass through unchanged.
  intermediate_types <- lapply(orig_types, function(t) {
    inter <- .at_intermediate(t)
    if (is.null(inter)) {
      .at_cast(
        t,
        convert_uint = convert_uint,
        session_timezone = session_timezone
      )
    } else {
      inter
    }
  })

  needs_intermediate <- any(sapply(orig_types, function(t) {
    !is.null(.at_intermediate(t))
  }))

  if (needs_intermediate) {
    return(list(make_schema(intermediate_types), make_schema(final_types)))
  }
  return(list(make_schema(final_types)))
}
.af_cast <- function(
  af,
  convert_uint = TRUE,
  session_timezone = NULL
) {
  rs <- af$schema
  rsl <- .sch_cast(
    rs,
    convert_uint = convert_uint,
    session_timezone = session_timezone
  )
  for (i in seq_along(rsl)) {
    af <- af$cast(rsl[[i]])
  }
  return(af)
}

### Array from text ----
.split_txt_array <- function(x, type, tz = NA_character_) {
  y <- gsub("(^[[]|[]]$)", "", x)
  y <- strsplit(y, split = ifelse(type == "character", "','", ","))
  y <- lapply(y, function(z) sub("(^'|'$)", "", z))
  if (type == "Date") {
    y <- lapply(y, as.Date)
  }
  if (type == "POSIXct") {
    y <- if (is.na(tz)) {
      lapply(y, function(x) {
        z <- as.POSIXct(x, tz = "UTC")
        attr(z, "tzone") <- NULL
        z
      })
    } else {
      lapply(y, as.POSIXct, tz = tz)
    }
  }
  if (!type %in% c("character", "Date", "POSIXct")) {
    y <- lapply(y, as, class = type)
  }
  return(y)
}

### Extract explicit timezone from a ClickHouse type string ----
## e.g. "DateTime('Europe/Paris')" or "Nullable(DateTime64(3, 'UTC'))".
## The quotes may come back backslash-escaped (e.g. "DateTime(\'UTC\')") since
## this is parsed out of the raw TSV header line rather than actual TSV data.
.type_timezone <- function(chClasses) {
  pattern <- "\\\\?'([^'\\\\]*)\\\\?'"
  vapply(
    chClasses,
    function(s) {
      m <- regmatches(s, regexec(pattern, s))[[1]]
      if (length(m) < 2) NA_character_ else m[2]
    },
    character(1),
    USE.NAMES = FALSE
  )
}
