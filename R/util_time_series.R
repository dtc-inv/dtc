# Cut Time-series by Dates ----

#' @title Truncate time-series with start and end dates
#' @param x xts object
#' @param date_start optional beginning date to truncate time-series
#' @param date_end optional ending date to truncate time-series
#' @details
#' If neither date_start or date_end are entered the original
#' time series, x, will be returned. The date_start and date_end
#' parameters are inclusive, i.e., the date_start will be the first
#' date in the time-series. Note dates can be Date objects or "YYYY-MM-DD"
#' strings.
#' @return truncated xts
#' @examples
#' data(assets)
#' # cut time-series to start in Feb 1, 2014 and end on June 30, 2014
#' x_cut <- cut_time(x, '2014-02-01', '2014-06-30')
#' head(x)
#' tail(x)
#' # leave beginning of time-serie as is, end on June 30, 2014
#' cut_time(x, NULL, '2014-06-30')
#' tail(x)
#' @export
cut_time <- function(x, date_start = NULL, date_end = NULL) {
  if (is.null(date_start) & is.null(date_end)) {
    return(x)
  }
  if (is.null(date_start)) {
    return(x[paste0('/', date_end)])
  }
  if (is.null(date_end)) {
    return(x[paste0(date_start, '/')])
  }
  return(x[paste0(date_start, '/', date_end)])
}

#' @title Find intersect start date
#' @param x xts
#' @return Date representing the common start date of multiple assets.
#' @examples
#' data(assets)
#' x[1:3, 1] <- NA
#' head(x)
#' first_comm_start(x)
#' @export
first_comm_start <- function(x) {
  first_days <- rep(NA, ncol(x))
  for (i in 1:ncol(x)) {
    first_days[i] <- zoo::index(na.omit(x[, i]))[1]
  }
  return(max(as.Date(first_days, origin = '1970-01-01')))
}

#' @title Find intersect end date
#' @param x xts
#' @return Date representing common ending date of multiple assets.
#' @examples
#' data(assets)
#' x[nrow(x), 1] <- NA
#' tail(x)
#' last_comm_end(x)
#' @export
last_comm_end <- function(x) {
  last_days <- rep(NA, ncol(x))
  for (i in 1:ncol(x)) {
    last_days[i] <- zoo::index(na.omit(x[, i]))[nrow(na.omit(x[, i]))]
  }
  return(min(as.Date(last_days, origin = "1970-01-01")))
}

#' @title Get Start and End Dates for each Asset in xts
#' @param x xts
#' @return data.frame with start and end dates for each column in xts
#' @examples
#' data(assets)
#' ret_date_info(x)
#' @export
ret_date_info <- function(x) {
  sdate <- rep(NA, ncol(x))
  edate <- sdate
  for (i in 1:ncol(x)) {
    dt <- zoo::index(na.omit(x[, i]))
    if (length(dt) == 0) {dt <- NA}
    sdate[i] <- dt[1]
    edate[i] <- dt[length(dt)]
  }
  nm <- colnames(x)
  if (is.null(nm)) {
    nm <- 1:ncol(x)
  }
  data.frame(Name = nm, Start = as.Date(sdate), End = as.Date(edate))
}

# Dataframe, matrix, and xts conversion ----

#' @title Convert dataframe to xts
#' @param xdf data.frame with dates in first column
#' @return xts
#' @examples
#' data(assets)
#' my_df <- xts_to_dataframe(x)
#' my_xts <- dataframe_to_xts(my_df)
#' head(my_df)
#' head(my_xts)
#' @export
dataframe_to_xts <- function(xdf) {
  ix <- is.na(xdf[[1]])
  xdf <- xdf[!ix, ]
  res <- xts(xdf[, -1], as.Date(xdf[[1]]))
  colnames(res) <- colnames(xdf)[-1]
  res
}

#' @title Convert xts object to a data.frame
#' @param x xts object
#' @return data.frame with Dates in the first column
#' @export
xts_to_dataframe <- function(x) {
  date_vec <- zoo::index(x, origin = '1970-01-01')
  xdf <- data.frame(Date = as.Date(date_vec), x, row.names = NULL)
  if (!is.null(colnames(x))) {
    colnames(xdf)[2:ncol(xdf)] <- colnames(x)
  }
  return(xdf)
}

#' @title Convert matrix to xts object
#' @param x matrix with dates in first column
#' @return xts
#' @export
mat_to_xts <- function(x) {
  xts(x[, -1], as.Date(x[[1]]))
}

#' @title Convert xts to tidy (long) data.frame
#' @param x xts
#' @examples
#' data(assets)
#' tidy_df <- xts_to_tidy(x)
#' head(tidy_df)
#' @export
xts_to_tidy <- function(x) {
  xdf <- xts_to_dataframe(x)
  tidyr::pivot_longer(xdf, -Date)
}

# Combine xts ----

#' @title Row bind two xts objects
#' @param new xts with more recent time-series
#' @param old xts with older time-series
#' @param is_xts boolean, default is TRUE for new and old to be xts objects.
#'   If set to FALSE, assumes `new` and `old` are data.frames with dates in
#'   first column named `Date`. See `xts_to_dataframe` and `dataframe_to_xts`
#' @param backfill if set to TRUE missing values of new will be filled with old
#' @details
#' For any overlapping dates, the old xts will be overwritten by the new xts.
#' The function will align the intersection of column names for the old and new
#' xts objects. If backfill is set to TRUE, then missing new values will be
#' over-written with the overlapping old data. For any column names that
#' do not intersect NAs will be added to the new or old xts depending on which
#' xts is missing the data.
#' @return xts with combined rows
#' @export
xts_rbind <- function(new, old, is_xts = TRUE, backfill = FALSE) {
  if (is_xts) {
    new_df <- xts_to_dataframe(new)
    old_df <- xts_to_dataframe(old)
  } else {
    new_df <- new
    old_df <- old
  }
  new_df$Date <- as.character(new_df$Date)
  old_df$Date <- as.character(old_df$Date)
  new_ldf <- pivot_longer(new_df, cols = -Date, names_to = "name",
                          values_to = "value")
  old_ldf <- pivot_longer(old_df, cols = -Date, names_to = "name",
                          values_to = "value")
  if (backfill) {
    new_ldf <- na.omit(new_ldf)
  }
  new_id <- paste0(new_ldf$Date, new_ldf$name)
  old_id <- paste0(old_ldf$Date, old_ldf$name)
  # find new ids place in old ids, replace with new values
  ix <- na.omit(match(new_id, old_id))
  if (length(ix) > 0) {
    old_ldf <- old_ldf[-ix, ]
  }
  combo <- rbind(new_ldf, old_ldf)
  combo_w <- pivot_wider(combo, id_cols = Date, names_from = name,
                         values_from = value)
  return(dataframe_to_xts(combo_w))
}


#' @title Column bind the intersection of two xts objects
#' @param x xts object
#' @param y xts object
#' @details
#' The first common start date and last common end date are used to find the
#' interection of returns. The returns are also cleaned for missing columns.
#' See `clean_ret` for more info.
#' @return list containing intersection of returns and columns that were
#'   removed due to too many missing values (if any).
#' @export
xts_cbind_inter <- function(x, y, eps = 0.05) {
  combo <- cbind.xts(x, y, check.names = FALSE)
  colnames(combo) <- c(colnames(x), colnames(y))
  res <- clean_ret(combo, eps = eps)
  return(res)
}

# Price to return conversions ----

#' @title Convert price time-series to a return
#' @param x xts of prices
#' @details
#' Return is defined by period over period change.
#' @return xts of returns
#' @examples
#' data(assets)
#' price <- ret_to_price(x[, 1:3])
#' head(price)
#' ret <- price_to_ret(price)
#' head(ret)
#' @export
price_to_ret <- function(x) {
  ret <- x / lag.xts(x, 1) - 1
  ret[2:nrow(ret), ]
}

#' @title Convert returns to a price index
#' @param x xts of returns
#' @return a price index calculating from \code{x} and a initial value of 1
#' #' @examples
#' data(assets)
#' price <- ret_to_price(x[, 1:3])
#' head(price)
#' ret <- price_to_ret(price)
#' head(ret)
#' @export
ret_to_price <- function(x) {
  price <- apply(x + 1, 2, cumprod)
  first_row <- xts(matrix(1, ncol = ncol(x)),
                   last_us_trading_day(zoo::index(x)[1]))
  price_out <- rbind(first_row, price)
  colnames(price_out) <- colnames(x)
  return(price_out)
}

# Trading days and freq ----

#' @title Get US Trading Days based on NYSE Holidays
#' @param date_start beginning date of sequence
#' @param date_end last date of sequence
#' @return sequence of trading days
#' @examples
#' us_trading_days("2020-01-01", "2020-01-07")
#' @export
us_trading_days <- function(date_start = NULL, date_end = NULL) {
  if (is.null(date_start)) date_start <- as.Date('1970-01-01')
  if (is.null(date_end)) date_end <- Sys.Date()
  all_days <- seq.Date(date_start, date_end, 'days')
  year_start <- lubridate::year(date_start)
  year_end <- lubridate::year(date_end)
  holidays <- timeDate::holidayNYSE(year_start:year_end)
  busday <- timeDate::isBizday(timeDate::timeDate(all_days),
                               as.Date(holidays@Data))
  all_days[busday]
}

#' @title Get Most Recent U.S. Trading Day
#' @param as_of optional as of date, leave NULL for today.
#' @details Returns the most recent completed trading day. E.g., if yesterday
#' was a trading day then it will return yesterday.
#' @export
last_us_trading_day <- function(as_of = NULL) {
  if (is.null(as_of)) {
    as_of <- Sys.Date()
  }
  yr <- lubridate::year(as_of)
  bizdays::create.calendar('cal',
                           holidays = timeDate::holidayNYSE((yr-1):(yr+1)),
                           weekdays = c('saturday', 'sunday'))
  bizdays::adjust.previous(as_of - 1, 'cal')
}

#' @title Change time-series frequency
#' @param x xts object
#' @param freq character string of the desired time-series periods: days,
#'   weeks, months, quarters, or years
#' @param dtype character string of "return" or "price" to represent the
#'   data type
#' @return xts object with new frequency
#' @importFrom lubridate ceiling_date
#' @details most recent period will be all the returns available for that
#'   period. E.g., if changing to annual returns the last year will be YTD.
#'   If the first year of data is not for a full year it will not be included.
#' @examples
#' data(assets)
#' # annual returns
#' x <- x["2020-06/", 1:3]
#' head(x)
#' tail(x)
#' change_freq(x, freq = "years")
#' @export
change_freq <- function(x, freq = "months", dtype = c("return", "price")) {
  freq <- check_freq(freq)
  dtype = tolower(dtype[1])
  if (dtype == "return") {
    res <- period.apply(x, endpoints(x, freq), colProd)
    res[res == 0] <- NA
  } else if (dtype == "price") {
    res <- to.period(x, freq)
  } else {
    error("incorrect dtype, must be 'return' or 'price'")
  }
  if (freq %in% c("months", "quarters", "years")) {
    res <- xts_eo_month(res)
  }
  return(res)
}

#' @title Convert dates to end of month
#' @param x date or vector of dates
#' @return dates shifted to end of month
#' @examples
#' eo_month(as.Date("2024-11-29"))
#' @export
eo_month <- function(x) {
  lubridate::ceiling_date(x, "months") - 1
}

#' @title Convert xts index to end of month dates
#' @param x xts object
#' @return xts object with end of month dates as index
#' @export
xts_eo_month <- function(x) {
  zoo::index(x) <- eo_month(zoo::index(x))
  return(x)
}

#' @title Calculate product of each column
#' @param x matrix, dataframe, or xts with numeric columns
#' @param add_1 boolean to add 1 for geometric product of returns
#' @param na_rm boolean to remove NA values, passed through to na.rm of prod
#'   function
#' @return vector of column products
#' @examples
#' data(assets)
#' colProd(x)
#' @export
colProd <- function(x, add_1 = TRUE, na_rm = TRUE) {
  if (add_1) {
    x <- x + 1
  }
  res <- apply(x, 2, prod, na.rm = na_rm)
  if (add_1) {
    res <- res - 1
  }
  return(res)
}


#' @title Change Character Frequency into Numeric Scaler
#' @param freq string: days, weeks, months, quarters, years
#' @return corresponding numeric value, e.g., months = 12
#' @examples
#' freq_to_scaler("months")
#'
#' @export
freq_to_scaler <- function(freq) {
  period <- check_freq(freq)
  switch(tolower(freq),
         days = 252,
         weeks = 52,
         months = 12,
         quarters = 4,
         years = 1
  )
}

# Read in xts from Excel ----

#' @title Read excel time-series
#' @param wb workbook full file name, e.g.,
#'   'C:/users/asotolongo/documents/wb.xslx'
#' @param sht worksheet number or name
#' @param skip number of header rows to skip, default is 0
#' @details
#' See `readxl::read_excel` for more info. Format of workbook needs to have
#'   date column in first row.
#' @export
read_xts <- function(wb, sht = 1, skip = 0) {
  dat <- readxl::read_excel(wb, sheet = sht, col_types = 'numeric', skip =
                              skip)
  xts(dat[, -1], as.Date(dat[[1]], origin = '1899-12-30'))
}

# Cleaning ----


#' @title Handle combing and cleaning asset, rf, and benchmarks
#' @param x xts object of asset(s)
#' @param b xts object of benchmark(s)
#' @param rf optional xts object of risk-free time-series
#' @param freq string return frequency: days, weeks, months, quarters, years,
#'   see details
#' @param date_start date representing first date. See details.
#' @param date_end date representing most recent date. See details.
#' @param eps percent of missing values (NAs) to allow, default is 0.05 for 5%.
#'   See details.
#' @details
#'   Utility function to handle common task of needing to line up asset, bench,
#'   and rf returns. If left `NULL` the `date_start` and `date_end` will
#'   truncate the data at the most recent common start date and oldest common
#'   end date. If further truncation is desired dates can be entered to cut the
#'   time-series accordingly. `eps` sets the bar for how many missing values
#'   are allowed per column. The default is 5%, if a column has more than 5%
#'   of the observations are missing the column is removed. Otherwise the
#'   missing values are filled with zero. If frequency is entered returns will
#'   be changed to desired frequency, otherwise leave `NULL` to not alter.
#' @return list with $x for asset(s), $b for benchmark(s), $xb for assets and
#'  benchmark, and $rf if risk-free is entered.
#' @examples
#' data(assets)
#' b <- x[, 1]
#' x <- x[, -1]
#' res <- clean_asset_bench_rf(x, b, rf = NULL, freq = "months",
#'   date_start = "2020-01-01")
#' head(res$x)
#' @export
clean_asset_bench_rf <- function(x, b = NULL, rf = NULL, freq = NULL,
                                 date_start = NULL, date_end = NULL,
                                 eps = 0.05, add_bench_nm = TRUE,
                                 add_rf_nm = TRUE) {
  if (!is.null(freq)) {
    x <- change_freq(x, freq)
    if (!is.null(b)) {
      b <- change_freq(b, freq)
    }
    if (!is.null(rf)) {
      rf <- change_freq(rf, freq)
    }
  }
  if (add_bench_nm && !is.null(b)) {
    colnames(b) <- paste0("Bench: ", colnames(b))
  }
  if (add_rf_nm && !is.null(rf)) {
    colnames(rf) <- paste0("Rf: ", colnames(rf))
  }
  combo <- xts_cbind_inter(x, b, eps)
  if (!is.null(colnames(combo$miss))) {
    if (colnames(b) %in% colnames(combo$miss)) {
      stop('benchmark is missing')
    }
    if (all(colnames(x) %in% colnames(combo$miss))) {
      stop("all assets are missing")
    }
  }
  if (!is.null(rf)) {
    combo <- xts_cbind_inter(combo$ret, rf, eps)
    if (!is.null(colnames(combo$miss))) {
      if (colnames(rf) %in% colnames(combo$miss)) {
        stop('rf is missing')
      }
    }
  }
  if (!is.null(date_start)) {
    combo$ret <- cut_time(combo$ret, date_start = date_start)
  }
  if (!is.null(date_end)) {
    combo$ret <- cut_time(combo$ret, date_end = date_end)
  }
  res <- list()
  if (!is.null(rf)) {
    res$rf <- combo$ret[, colnames(combo$ret) %in% colnames(rf)]
  }
  res$b <- combo$ret[, colnames(combo$ret) %in% colnames(b)]
  res$x <- combo$ret[, colnames(combo$ret) %in% colnames(x)]
  res$xb <- cbind.xts(res$x, res$b, check.names = FALSE)
  return(res)
}

#' @title Clean returns with missing values
#' @param x xts to clean
#' @param trunc_start boolean to truncate at intersection of start dates
#' @param trunc_end boolean to truncate at intersection of end dates
#' @param eps threshold of missing values tolerated. See details and
#'   examples.
#' @param ret_fill numeric value to fill missing returns within threshold,
#'   see details.
#' @details Default `eps` is 0.05 meaning that if more than 5% of the number
#'   of observations (rows) for each field (column) are missing then the
#'   column is removed. Otherwise, if 5% or less of observations are missing,
#'   then missing values are filled with `ret_fill` (zero by default).
#' @examples
#' x <- x[1:10, 1:3]
#' x[2:6, 1] <- NA
#' x
#' res <- clean_ret(x, eps = 0.05)
#' res$ret
#' res$miss
#' @export
clean_ret <- function(x, trunc_start = TRUE, trunc_end = TRUE, eps = 0.05,
                      ret_fill = 0) {
  if (trunc_start) {
    x <- x[paste0(first_comm_start(x), "/")]
  }
  if (trunc_end) {
    x <- x[paste0("/", last_comm_end(x))]
  }
  miss_col <- colSums(is.na(x)) > floor(eps * nrow(x))
  x[is.na(x)] <- ret_fill
  if (all(miss_col)) {
    ret <- xts()
    miss <- x
    warning("all returns are missing")
  } else if (any(miss_col)) {
    ret <- x[, !miss_col]
    miss <- x[, miss_col]
    miss_nm <- paste0(paste0(colnames(x)[miss_col], collapse = " ,"))
    warning(
      paste0(
        miss_nm,
        " exceeded eps threshold for missing observations."
      )
    )
  } else {
    miss <- xts()
    ret <- x
  }
  res <- list()
  res$ret <- ret
  res$miss <- miss
  return(res)
}

#' @title Fill Missing Price Data with Last Found Observation
#' @param x xts
#' @details
#' For any missing data in a price time-series, found with `is.na`, the
#' last known price will be forward filled. E.g., for a monthly time-series
#' if Feb is missing than the Jan price will be filled in Feb.
#' @returns xts with missing data filled
#' @export
fill_na_price <- function(x) {
  x_fill <- apply(x, 2, na_price)
  xts(x_fill, zoo::index(x))
}

#' @title Fill Missing Price Data for a Vector
#' @param x vector of price data
#' @details see `fill_na_price`, which applies this function to an xts
#' @export
na_price <- function(x) {
  ind <- which(!is.na(x))
  if (is.na(x[1])) {
    ind <- c(1, ind)
  }
  rep(x[ind], times = diff(c(ind, length(x) + 1)))
}

#' @title Guess Frequency with xts periodicity
#' @param x xts
#' @return frequency as string, e.g., "days", "months"
#' @export
#' @examples
#' data(assets)
#' guess_freq(x)
guess_freq <- function(x) {
  freq <- try(periodicity(x))
  if ("try-error" %in% class(x)) {
    warning("Could not guess frequency.")
    return(NA)
  } else {
    freq <- tolower(freq$scale)
    if (freq == "daily") {
      return("days")
    }
    gsub("ly", "s", freq)
  }
}

#' @title Check Frequency Input
#' @param freq frequency string: days, weeks, months, quarters, or years
#' @return if valid frequency, else throws error
#' @examples
#' check_freq("Months")
#' check_freq("M")
#' @export
check_freq <- function(freq) {
  val <- c("days", "weeks", "months", "quarters", "years")
  if (!tolower(freq) %in% val) {
    stop(paste0(freq, " misspecified. Needs to be one of: ",
                paste0(val, collapse = ", ")))
  } else {
    return(tolower(freq))
  }
}

#' @title Clean Returns by Column
#' @description Handle missing returns in each column
#'   without truncating to common periods for the return matrix. If the
#'   % of missing values (once data starts in each column) is over the eps
#'   the column will be removed, otherwise missing values will be replaced
#'   with zeros.
#' @param x return xts
#' @param eps % of missing returns to tolerate, default is 0.05 for 5%
#' @return list with $ret for cleaned returns and $miss for any columns
#'   that had to be removed (if any)
#' @export
clean_by_col <- function(x, eps = 0.05) {
  is_miss <- rep(NA, ncol(x))
  for (i in 1:ncol(x)) {
    cln <- na.omit(x[, i])
    first_ret <- zoo::index(cln)[1]
    last_ret <- zoo::index(cln)[length(cln)]
    dt_rng <- paste0(first_ret, "/", last_ret)
    is_miss[i] <- sum(is.na(x[dt_rng, i])) > (eps * length(x[dt_rng, i]))
    if (!is_miss[i]) {
      x_sub <- x[dt_rng, i]
      x_sub[is.na(x_sub)] <- 0
      x[dt_rng, i] <- x_sub
    }
  }
  if (any(is_miss)) {
    ret <- x[, !is_miss]
    miss <- x[, is_miss]
  } else {
    ret <- x
    miss <- NA
  }
  res <- list()
  res$ret <- ret
  res$miss <- miss
  return(res)
}
