#' @export
run_ppu <- function(bucket, as_of = NULL) {
  if (is.null(as_of)) {
    as_of <- last_us_trading_day(Sys.Date())
  } else {
    as_of <- as.Date(as_of)
  }
  ppu_tbl <- try_read(bucket, "meta-tables/ppu.parquet")
  r <- read_ret(ppu_tbl$DtcName, bucket)
  r <- cut_time(r, date_end = as_of)
  tbl_name <- unique(ppu_tbl$Table)
  res <- list()
  for (i in 1:length(tbl_name)) {
    res[[i]] <- ppu_sect(ppu_tbl, r, as_of, tbl_name[i])
  }
  names(res) <- tbl_name
  out <- list()
  out$res <- res
  out$date_end <- zoo::index(r)[nrow(r)]
  return(out)
}

#' @export
run_ppu_monthly <- function(bucket, as_of = NULL) {
  if (is.null(as_of)) {
    as_of <- floor_date(Sys.Date(), "months") - 1
  }
  ppu_tbl <- try_read(bucket, "meta-tables/ppu-monthly.parquet")
  r <- read_ret(ppu_tbl$DtcName, bucket)
  r <- cut_time(r, date_end = as_of)
  tbl_name <- unique(ppu_tbl$Table)
  res <- list()
  for (i in 1:length(tbl_name)) {
    res[[i]] <- ppu_sect(ppu_tbl, r, as_of, tbl_name[i], eom = TRUE)
  }
  names(res) <- tbl_name
  out <- list()
  out$res <- res
  out$date_end <- zoo::index(r)[nrow(r)]
  return(out)
}

#' @export
ppu_sect <- function(ppu_tbl, r, as_of, tbl_name = NULL, eom = FALSE) {
  if (!is.null(tbl_name)) {
    ppu_x <- ppu_tbl[ppu_tbl$Table %in% tbl_name, ]
  } else {
    ppu_x <- ppu_tbl
  }
  x <- r[, ppu_x$DtcName]
  x <- clean_by_col(x)
  if (!is.na(x$miss)) {
    warning("some returns missing")
    ppu_x <- ppu_x[ppu_x$DtcName %in% colnames(x$ret), ]
  }
  x <- x$ret
  perf <- tbl_cal_perf(x, as_of, eom)
  perf$Name <- gsub(" Daily Est.", "", perf$Name)
  yrs <- xts_to_dataframe(change_freq(x, "years"))
  yrs <- yrs[order(yrs$Date, decreasing = TRUE), ]
  yrs <- t(yrs[2:4, -1])
  y <- seq(from = year(as_of) - 1, to = year(as_of) - 3)
  colnames(yrs) <- as.character(y)
  xdf <- data.frame(
    Name = perf$Name, Target = ppu_x$Target / 100
  )
  xdf <- cbind(xdf, perf[, 2:8], yrs)
  xdf <- cbind(xdf, ppu_x[, c("Format", "DisplayName")])
  colnames(xdf)[13] <- "Format"
  xdf$Format[is.na(xdf$Format)] <- ""
  xdf <- xdf[, c("DisplayName", colnames(xdf)[2:(ncol(xdf)-1)])]
  colnames(xdf)[1] <- "Name"
  rownames(xdf) <- NULL
  column_defs <- setNames(
    lapply(colnames(xdf[, -1]), function(col) {
      if (col %in% c("DTD", "1 Yr", "2024")) {
        colDef(
          style = function(value, index, name) {
            list(borderLeft = "1px solid #000")
          },
          format = colFormat(percent = TRUE, digits = 2)
        )
      } else {
        colDef(
          format = colFormat(percent = TRUE, digits = 2)
        )
      }
    }),
    colnames(xdf)[-1]
  )
  column_defs$Format <- colDef(show = FALSE)
  column_defs$Name <- colDef(width = 200, sticky = "left")
  rdf <- reactable(
    xdf,
    pagination = FALSE,
    sortable = FALSE,
    theme = reactableTheme(
      headerStyle = list(
        backgroundColor =  "#003057",
        color = "white",
        fontWeight = "bold"
      )
    ),
    columns = column_defs,
    rowStyle = function(index) {
      if (xdf[index, "Format"] == "bold") {
        list(`font-weight` = "bold")
      } else if (xdf[index, "Format"] == "bench") {
        list(background = "lightgray")
      } else if (xdf[index, "Format"] == "lowgrid") {
        list(borderBottom = "1px solid #000")
      } else if (xdf[index, "Format"] == "lowgrid-bench") {
        list(background = "lightgray", borderBottom = "1px solid #000")
      }
    }
  )
  res <- list()
  res$react <- rdf
  res$num_df <- xdf
  return(res)
}

#' @export
tbl_cal_perf <- function(x, as_of = NULL, eom = FALSE) {
  dt <- eom_cal_perf_dt(as_of, eom)
  perf <- list()
  for (i in 1:length(dt)) {
    perf[[i]] <- apply(x[paste0(dt[i], "/", as_of)] + 1, 2, prod)
    if (i %in% 6:8) {
      a <- as.numeric(gsub(" Yr", "", names(dt[i])))
      perf[[i]] <- perf[[i]]^(1/a) - 1
    } else {
      perf[[i]] <- perf[[i]] - 1
    }
  }
  perf <- do.call("cbind", perf)
  perf <- data.frame(
    Name = rownames(perf),
    perf,
    row.names = NULL
  )
  colnames(perf)[2:ncol(perf)] <- names(dt)
  return(perf)
}

#' @title Get Common Trailing Performance Dates
#' @param as_of starting date, default is end of last month
#' @param eom option to use end of month dates if working with monthly data,
#'   default is TRUE
#' @return vector of dates for common trailing periods, see details
#' @details
#'   Will return DTD, MTD, QTD, 1 Yr, 3 Yr, 5 Yr, and 10 Yr trailing periods.
#'   If \code{eom} parameter is set to TRUE then DTD will be omitted.
#' @export
eom_cal_perf_dt <- function(as_of = NULL, eom = TRUE) {
  if (is.null(as_of)) {
    as_of <- lubridate::floor_date(Sys.Date(), "months") - 1
  } else {
    as_of <- try(as.Date(as_of))
    if (inherits(as_of, "try-error")) {
      stop("as_of could not be converted to date")
    }
  }
  dt <- c(
    as_of,
    floor_date(as_of, "months"),
    floor_date(as_of, "quarters"),
    floor_date(as_of, "years"),
    as_of - years(1),
    as_of - years(3),
    as_of - years(5),
    as_of - years(10)
  )
  nm <- c("DTD", "MTD", "QTD", "YTD", "1 Yr", "3 Yr", "5 Yr", "10 Yr")
  if (eom) {
    dt <- eo_month(dt)
    dt[5:8] <- eo_month(add_with_rollback(dt[5:8], months(1)))
    names(dt) <- nm
    dt <- dt[-1]
    return(dt)
  } else {
    names(dt) <- nm
    dt[5:8] <- next_trading_day(dt[5:8], 1)
    return(dt)
  }
}
