
#' @title Space Out ids in multiple iterations
#' @param ids vector of ids
#' @param max_ids max number of ids per iteration
#' @details utility function for factset download that limits number of ids per
#'  api call
#' @return iteration index
#' @export
space_ids <- function(ids, max_ids = 50) {
  if (length(ids) > max_ids) {
    iter <- iter <- seq(1, length(ids), (max_ids - 1))
    if (iter[length(iter)] < length(ids)) {
      iter <- c(iter, length(ids))
    }
    ret_list <- list()
  } else {
    iter <- c(1, length(ids))
  }
  return(iter)
}

#' @title Clean CUSIPs in holdings tables
#' @param tbl_hold data.frame with holdings
#' @description CUSIPs with all zeros need to be read as NA
#' @return tbl_hold with cleaned up CUSIPs
#' @export
clean_ids <- function(tbl_hold) {
  if ("Cusip" %in% colnames(tbl_hold)) {
    ix <- tbl_hold[, "Cusip"] == "000000000"
    if (any(na.omit(ix))) {
      tbl_hold[ix, "Cusip"] <- NA
    }
  }
  return(tbl_hold)
}

#' @title Find IDs in holdings table from multiple fields
#' @description will look for DtcName, Ticker, Cusip, Sedol, Lei, and
#'   Identifier fields to combine into an "IDs" vector
#' @param tbl_hold data.frame with holdings
#' @return vector of IDs
#' @export
get_ids <- function(tbl_hold) {
  tbl_hold <- clean_ids(tbl_hold)
  id_field <- c("DtcName", "Ticker", "Cusip", "Sedol", "Lei", "Identifier")
  id_bool <- id_field %in% colnames(tbl_hold)
  if (!any(id_bool)) {
    stop("no id fields found")
  }
  tbl_id <- tbl_hold[, id_field[id_bool], drop = FALSE]
  ids <- tbl_id[, 1]
  if (ncol(tbl_id) > 1) {
    for (i in 2:ncol(tbl_id)) {
      ids[is.na(ids)] <- tbl_id[, i][is.na(ids)]
    }
  }
  ids <- unique(ids)
  return(ids)
}

#' @title Utility function to extract a named field from a list
#'  while returning NA instead of NULL values for missing data
#' @param x list
#' @param nm vector of names (characters)
#' @return values in list that correspond to the named field
#' @export
extract_list <- function(x, nm) {
  y <- lapply(x, '[[', nm)
  y[sapply(y, is.null)] <- NA
  unlist(y)
}

#' @title Utility function to replace NULL values in list with NA
#' @param x list
#' @export
list_replace_null <- function(x) {
  x[sapply(x, is.null)] <- NA
  return(x)
}

#' @title Get IDs from multiple fields in Master Security List
#' @description will search (in order): Isin, Sedol, Lei, Cusip, Ticker,
#'   and Identifier. For use with Factset downloads requiring these types of
#'   IDs.
#' @param tbl_msl data.frame containing MSL
#' @return vector of ids
#' @export
create_ids <- function(tbl_msl) {
  ids <- tbl_msl$Isin
  ids[is.na(ids)] <- tbl_msl$Sedol[is.na(ids)]
  ids[is.na(ids)] <- tbl_msl$Lei[is.na(ids)]
  ids[is.na(ids)] <- tbl_msl$Cusip[is.na(ids)]
  ids[is.na(ids)] <- tbl_msl$Ticker[is.na(ids)]
  ids[is.na(ids)] <- tbl_msl$Identifier[is.na(ids)]
  return(ids)
}

#' @title Utility function to fill missing values
#' @description Fills missing values of vector a with vector b, where a
#'   and b are equal lengths with corresponding values, e.g., Tickers and
#'   CUSIPS for the same securities
#' @param a starting vector with potentially missing values
#' @param b corresponding vector of the same length used to fill parts of a
#'   that are missing values
#' @return a with missing values filled by b
#' @note if a and b are not the same length the function will return a as
#' originally inputted
#' @export
fill_ix <- function(a, b) {
  if (length(a) == length(b)) {
    a[is.na(a)] <- b[is.na(a)]
    return(a)
  } else {
    warning("a and b were different lengths, returning a")
    return(a)
  }
}

#' @export
match_ids_dtc_name <- function(ids, tbl_msl) {
  incomps <- c(NA, "000000000", "N/A", "0")
  ix_dtc <- match(ids, tbl_msl$DtcName, incomparables = incomps)
  ix_ticker <- match(ids, tbl_msl$Ticker, incomparables = incomps)
  ix_isin <- match(ids, tbl_msl$Isin, incomparables = incomps)
  ix_cusip <- match(ids, tbl_msl$Cusip, incomparables = incomps)
  ix_sedol <- match(ids, tbl_msl$Sedol, incomparables = incomps)
  ix_lei <- match(ids, tbl_msl$Lei, incomparables = incomps)
  ix_id <- match(ids, tbl_msl$Identifier, incomparables = incomps)
  ix <- rep(NA, length(ids))
  ix <- fill_ix(ix, ix_dtc)
  ix <- fill_ix(ix, ix_cusip)
  ix <- fill_ix(ix, ix_isin)
  ix <- fill_ix(ix, ix_sedol)
  ix <- fill_ix(ix, ix_lei)
  ix <- fill_ix(ix, ix_ticker)
  ix <- fill_ix(ix, ix_id)
  return(ix)
}

#' @export
match_mult <- function(x, y, match_by) {
  x <- as.data.frame(x)
  y <- as.data.frame(y)
  incomps <- c(NA, "000000000", "N/A", "0")
  ix <- rep(NA, nrow(x))
  for (i in 1:length(match_by)) {
    a <- try(x[, match_by[i]], silent = TRUE)
    if ("try-error" %in% class(a)) {
      warning(paste0(match_by[i], " not found in x"))
      a <- rep(NA, nrow(x))
    }
    b <- try(y[, match_by[i]], silent = TRUE)
    if ("try-error" %in% class(b)) {
      warning(paste0(match_by[i], " not found in y"))
      b <- rep(NA, nrow(y))
    }
    ix <- fill_ix(ix, match(a, b, incomparables = incomps))
  }
  return(ix)
}

#' @export
left_merge <- function(x, y, match_by, keep_x_dup_col = TRUE) {
  ix <- match_mult(x, y, match_by)
  if (keep_x_dup_col) {
    dup_col <- colnames(y) %in% colnames(x)
    tbl_union <- cbind(x, y[ix, !dup_col, drop = FALSE])
  } else {
    dup_col <- colnames(x) %in% colnames(y)
    tbl_union <- cbind(x[, !dup_col, drop = FALSE], y[ix, ])
  }
  tbl_inter <- tbl_union[!is.na(ix), ]
  tbl_miss <- tbl_union[is.na(ix), ]
  list(
    union = tbl_union,
    inter = tbl_inter,
    miss = tbl_miss
  )
}

#' @export
rob_rbind <- function(df1, df2) {
  if (nrow(df1) == 0) {
    return(df2)
  }
  if (nrow(df2) == 0) {
    return(df1)
  }
  nm_union <- unique(c(colnames(df1), colnames(df2)))
  df1_miss <- !nm_union %in% colnames(df1)
  df2_miss <- !nm_union %in% colnames(df2)
  df1[, nm_union[df1_miss]] <- NA
  df2[, nm_union[df2_miss]] <- NA
  df2 <- df2[, colnames(df1)]
  rbind(df1, df2)
}

#' @export
get_list_fld <- function(x, fld) {
  if (is.null(x[fld][[1]])) {
    return(NA)
  } else {
    x[fld]
  }
}


#' @export
remove_holding_dup <- function(tbl_hold, id = "Name") {
  is_dup <- duplicated(paste0(tbl_hold[, id], tbl_hold[, "TimeStamp"]))
  tbl_hold[!is_dup, ]
}

#' @export
avg_fina_ratio <- function(w, x, rm_negative = TRUE) {
  if (rm_negative) {
    x[x < 0] <- NA
  }
  is_miss <- is.na(x) | is.na(w)
  x[is_miss] <- NA
  w[is_miss] <- NA
  w <- w / sum(x, na.rm = TRUE)
  wgt_harmonic_mean(w, x)
}

#' @export
wgt_avg <- function(w, x) {
  is_miss <- is.na(x) | is.na(w)
  x[is_miss] <- NA
  w[is_miss] <- NA
  sum(x * w, na.rm = TRUE) / sum(w, na.rm = TRUE)
}

#' @title Check Holdings Table Specification
#' @param tbl_hold data.frame with holdings, need id, CapWgt, and TimeStamp
#' @export
check_tbl_hold <- function(tbl_hold) {
  if (!"data.frame" %in% class(tbl_hold)) {
    stop("holdings table is not a data.frame")
  }
  id_check <- any(c("DtcName", "Ticker", "Cusip", "Sedol", "Isin", "Lei",
                    "Identifier") %in% colnames(tbl_hold))
  wgt_check <- "CapWgt" %in% colnames(tbl_hold)
  time_check <- "TimeStamp" %in% colnames(tbl_hold)
  if (!id_check | !wgt_check | !time_check) {
    stop("Holdings table not properly specified. Need id, weight, and date")
  }
}

#' @export
next_trading_day <- function(dates, t_plus = 0) {
  yrs <- lubridate::year(dates)
  min_year <- min(yrs) - 1
  max_year <- max(yrs) + 1
  bizdays::create.calendar(
    "cal",
    holidays = timeDate::holidayNYSE(min_year:max_year),
    weekdays = c("saturday", "sunday"))
  bizdays::adjust.next(dates + t_plus, "cal")
}

#' @export
prev_trading_day <- function(dates, t_minus = 0) {
  yrs <- lubridate::year(dates)
  min_year <- min(yrs) - 1
  max_year <- max(yrs) + 1
  bizdays::create.calendar(
    "cal",
    holidays = timeDate::holidayNYSE(min_year:max_year),
    weekdays = c("saturday", "sunday"))
  bizdays::adjust.previous(dates - t_minus, "cal")
}

#' @export
mid_month <- function(dt) {
  dt <- lubridate::floor_date(dt, "months")
  prev_trading_day(dt + 14)
}

#' @export
match_bd_id <- function(bd_id, tbl_msl, ix = NULL) {
  incomps <- c(NA, "000000000", "N/A", "0")
  if (is.null(ix)) {
    ix <- rep(NA, length(bd_id))
  }
  ix <- fill_ix(ix, match(bd_id, tbl_msl$Sedol, incomparables = incomps))
  ix <- fill_ix(ix, match(bd_id, tbl_msl$Cusip, incomparables = incomps))
  ix <- fill_ix(ix, match(bd_id, tbl_msl$BdName, incomparables = incomps))
  return(ix)
}


#' @export
rbind_holdings <- function(old, new, keep = c("old", "new")) {
  keep <- tolower(keep[1])
  if (nrow(old) == 0) {
    return(new)
  }
  if (keep == "old") {
    ix <- new$TimeStamp %in% unique(old$TimeStamp)
    if (any(ix)) {
      new <- new[!ix, ]
    }
  }
  if (keep == "new") {
    ix <- old$TimeStamp %in% unique(new$TimeStamp)
    if (any(ix)) {
      old <- old[!ix, ]
    }
  }
  res <- rbind(old, new)
  return(res)
}

#' @export
xts_cbind <- function(x, y) {
  combo <- cbind.xts(x, y, check.names = FALSE)
  colnames(combo) <- c(colnames(x), colnames(y))
  return(combo)
}

#' @title Merge MSL with holdings table
#' @param tbl_hold data.frame with portfolio holdings
#' @param tbl_msl master security list (data.frame)
#' @return list with union, intersection, and missing tables
#' @export
merge_msl <- function(tbl_hold, tbl_msl, rm_dup_dates = TRUE) {
  ix <- match_mult(
    x = tbl_hold,
    y = tbl_msl,
    match_by = c("DtcName", "Ticker", "Cusip", "Sedol", "Isin", "Lei",
                 "Identifier")
  )
  if ("Identifier" %in% colnames(tbl_hold)) {
    ix <- match_bd_id(tbl_hold$Identifier, tbl_msl, ix)
    if ("Cusip" %in% colnames(tbl_hold)) {
      ix <- match_bd_id(tbl_hold$Cusip, tbl_msl, ix)
    }
    if ("Name" %in% colnames(tbl_hold)) {
      ix <- match_bd_id(tbl_hold$Name, tbl_msl, ix)
    }
  }
  y_dup_col <- colnames(tbl_msl) %in% colnames(tbl_hold)
  x_dup_col <- colnames(tbl_hold) %in% colnames(tbl_msl)
  tbl_union <- cbind(tbl_hold, tbl_msl[ix, !y_dup_col, drop = FALSE])
  tbl_miss <- tbl_hold[is.na(ix), ]
  tbl_inter <- cbind(tbl_hold[, !x_dup_col, drop = FALSE], tbl_msl[ix, ])
  tbl_inter <- tbl_inter[!is.na(ix), ]
  res <- list()
  res$inter <- tbl_inter
  res$union <- tbl_union
  res$miss <- tbl_miss
  if (rm_dup_dates) {
    is_dup <- duplicated(paste0(res$inter$DtcName, res$inter$TimeStamp))
    if (any(is_dup)) {
      warning("duplicate dates found, removing")
      res$inter <- res$inter[!is_dup, ]
    }
  }
  return(res)
}

#' @title Convert calendar return dataframe to vector
#' @export
cal_ret_to_vec <- function(xdf, top_down = TRUE) {
  if (top_down) {
    x <- xdf[1, ]
    for (i in 2:nrow(xdf)) {
      x <- c(x, xdf[i, ])
    }
  } else {
    x <- xdf[nrow(x), ]
    for (i in seq((nrow(xdf)-1), 1, -1)) {
      x <- c(x, xdf[i, ])
    }
  }
  return(as.numeric(x))
}

#' @export
pe_cf_twr <- function(wb, cf_sht = "cf", nav_sht = "nav") {
  cf <- readxl::read_excel(wb, cf_sht)
  cf <- dataframe_to_xts(cf)
  nav <- readxl::read_excel(wb, nav_sht)
  nav <- dataframe_to_xts(nav)
  inter <- intersect(colnames(cf), colnames(nav))
  cf <- cf[, inter]
  nav <- nav[, inter]
  mod_deitz <- list()
  tvpi <- rep(NA, ncol(nav))
  for (i in 1:ncol(nav)) {
    x <- cbind(cf[, i], nav[, i])
    q_ret <- na.omit(x[, 2])
    colnames(q_ret) <- colnames(cf[, i])
    q_nav <- q_ret
    is_pi <- cf[, i] < 0
    is_pi[is.na(pi)] <- FALSE
    is_distr <- cf[, i] > 0
    is_distr[is.na(is_distr)] <- FALSE
    pi <- sum(cf[is_pi, i])
    distr <- sum(cf[is_distr, i])
    last_nav <- q_ret[nrow(q_ret)]
    tvpi[i] <- (last_nav + distr) / -pi
    for (j in 1:nrow(q_ret)) {
      q <- zoo::index(q_ret)[j]
      y <- year(q)
      m <- month(q)
      qe <- paste0(y, "-", m)
      qs <- paste0(y, "-", m-2)
      wcf <- wgt_cf(x[paste0(qs, "/", qe), 1])
      cfx <- as.numeric(sum(x[paste0(qs, "/", qe), 1], na.rm = TRUE))
      if (j == 1) {
        v0 <- 0
      } else {
        v0 <- as.numeric(q_nav[j-1])
      }
      v1 <- as.numeric(q_nav[j])
      pnl <- v1 - v0 + cfx
      if (-wcf > v0) {
        wcf <- cfx
      }
      q_ret[j] <- pnl / (v0 - wcf )
    }
    mod_deitz[[i]] <- q_ret
  }
  twr <- do.call(cbind, mod_deitz)
  colnames(twr) <- colnames(nav)
  res <- list()
  res$twr <- twr
  res$tvpi <- tvpi
  return(res)
}

#' @export
wgt_cf <- function(cf) {
  cf <- na.omit(cf)
  if (nrow(cf) == 0) {
    return(0)
  }
  x <- 0
  for (i in 1:nrow(cf)) {
    if (month(zoo::index(cf)[i]) %in% c(1, 4, 7, 10)) {
      x <- x + as.numeric(cf[i]) * 65 / 90
    } else if (month(zoo::index(cf)[i]) %in% c(2, 5, 8, 11)) {
      x <- x + as.numeric(cf[i]) * 45 / 90
    } else {
      x <- x + as.numeric(cf[i]) * 15 / 90
    }
  }
  return(x)
}

