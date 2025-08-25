#' @export
extract_list <- function(x, nm) {
  y <- lapply(x, '[[', nm)
  y[sapply(y, is.null)] <- NA
  unlist(y)
}

#' @export
list_replace_null <- function(x) {
  x[sapply(x, is.null)] <- NA
  return(x)
}


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

#' @title Read MSL from Database
#' @param ac ArcticDB object
#' @return data.frame of MSL
#' @export
read_msl <- function(ac) {
  lib <- ac$get_library("meta-tables")
  lib$read("msl")$data
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

