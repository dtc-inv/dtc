#' @export
check_api_keys <- function(fpath = NULL) {
  if (!exists("api_keys")) {
    if (is.null(fpath)) {
      fpath <- "~/api_keys.RData"
    }
    load(fpath)
  }
}

#' @import arrow
#' @export
create_bucket <- function(api_keys) {
  s3_bucket(
    bucket = "dtc-par",
    access_key = api_keys$s3$access_key,
    secret_key = api_keys$s3$secret_key
  )
}

#' @export
try_read <- function(bucket, fpath, max_iter = 10) {
  for (i in 1:max_iter) {
    x <- try(read_parquet(bucket$path(fpath)))
    if (inherits(x, "data.frame")) {
      return(x)
    }
  }
}

#' @export
try_write <- function(bucket, dat, fpath, max_iter = 10) {
  for (i in 1:max_iter) {
    x <- try(write_parquet(dat, bucket$path(fpath)))
    if (!inherits(x, "try-error")) {
      return("success")
    }
  }
}

#' @export
read_ret_par <- function(bucket, ret_src, tgt_col = NULL, max_iter = 10) {
  fpath <- bucket$path(paste0("returns/", ret_src, ".parquet"))
  for (i in 1:max_iter) {
    x <- try(read_parquet(fpath, col_select = tgt_col))
    if (inherits(x, "data.frame")) {
      return(x)
    }
  }
}

#' @export
read_msl <- function(bucket) {
  try_read(bucket, "meta-tables/tbl_msl.parquet")
}

#' @title Read Returns
#' @param ids ticker, cusip, lei, dtc name, or identifier
#' @param bucket S3FileSystem object
#' @param tbl_msl option to pass through a custom master security list,
#'   leave NULL for default MSL
#' @return xts of time-series of ids, will warn if some ids are not found
#' @export
read_ret <- function(ids, bucket, tbl_msl = NULL) {
  if (is.null(tbl_msl)) {
    tbl_msl <- read_msl(bucket)
  }
  ids_dict <- filter(
    tbl_msl,
    DtcName %in% ids | Ticker %in% ids | Cusip %in% ids | Sedol %in% ids |
      Lei %in% ids |  Identifier %in% ids
  )
  found <- ids %in% ids_dict$DtcName | ids %in% ids_dict$Ticker |
    ids %in% ids_dict$Cusip | ids %in% ids_dict$Lei | ids %in% ids_dict$Lei |
    ids %in% ids_dict$Identifier
  if (all(!found)) {
    warning("no ids found")
    return(NULL)
  }
  if (any(!found)) {
    warning(paste0(ids[!found], " not found. "))
  }
  ret_src <- unique(na.omit(ids_dict$ReturnLibrary))
  ret_meta <- try_read(bucket, "meta-tables/ret-meta.parquet")
  ret_data <- left_join(data.frame(ReturnLibrary = ret_src),
                        ret_meta, by = "ReturnLibrary")
  res <- list()
  for (i in 1:length(ret_src)) {
    x_dict <- filter(ids_dict, ReturnLibrary %in% ret_src[i])
    record <- read_ret_par(bucket, ret_src[i],
                           tgt_col = c("Date", x_dict$DtcName))
    if (ncol(record) == 1) {
      warning(x_dict$DtcName)
      next
    }
    res[[i]] <- dataframe_to_xts(record)
  }
  if ("monthly" %in% ret_data$Freq) {
    for (i in 1:length(res)) {
      res[[i]] <- change_freq(res[[i]])
    }
  }
  if (length(res) == 1) {
    ret <- res[[1]]
  } else {
    ret <- do.call("cbind", res)
    nm <- unlist(lapply(res, colnames))
    colnames(ret) <- nm
  }
  ix <- match_ids_dtc_name(ids, tbl_msl)
  dtc_name <- tbl_msl$DtcName[na.omit(ix)]
  ret <- ret[, dtc_name[dtc_name %in% colnames(ret)]]
  return(ret)
}


#' @title Read holdings
#' @param ids vector of IDs, can be Ticker, Cusip, DtcName, or any ID field in
#'   the MSL
#' @param bucket S3FileSystem object
#' @param tbl_msl data.frame representing master security list
#' @return list with holdings files
#' @export
read_hold <- function(ids, bucket, tbl_msl = NULL) {
  if (is.null(tbl_msl)) {
    tbl_msl <- read_msl(bucket)
  }
  ids_dict <- filter(
    tbl_msl,
    DtcName %in% ids | Ticker %in% ids | Cusip %in% ids | Sedol %in% ids |
      Lei %in% ids |  Identifier %in% ids
  )
  found <- ids %in% ids_dict$DtcName | ids %in% ids_dict$Ticker |
    ids %in% ids_dict$Cusip | ids %in% ids_dict$Lei | ids %in% ids_dict$Lei |
    ids %in% ids_dict$Identifier
  if (all(!found)) {
    warning("no ids found")
    return(NULL)
  }
  if (any(!found)) {
    warning(paste0(ids[!found], " not found in msl. "))
  }
  res <- list()
  all_hold_files <- gsub("holdings/", "", bucket$ls("holdings/"))
  all_hold_files <- gsub(".parquet", "", all_hold_files)
  ix <- ids_dict$DtcName %in% all_hold_files
  if (all(!ix)) {
    stop("no holdings files for any of the ids")
  }
  if (any(!ix)) {
    warning(paste0(ids_dict$DtcName[!ix], " not found in holdings files."))
    ids_dict <- ids_dict[ix, ]
  }
  for (i in 1:nrow(ids_dict)) {
    x <- try_read(bucket, paste0("holdings/", ids_dict$DtcName[i], ".parquet"))
    if (is.null(x)) {
      warning("bucket issue with reading holdings")
      x <- data.frame()
    }
    res[[i]] <- x
  }
  names(res) <- ids_dict$DtcName
  return(res)
}

read_fina_hist <- function(ids, bucket, dtype = "PE", tbl_msl = NULL) {
  if (is.null(tbl_msl)) {
    tbl_msl <- read_msl(bucket)
  }
  ids_dict <- filter(
    tbl_msl,
    DtcName %in% ids | Ticker %in% ids | Cusip %in% ids | Sedol %in% ids |
      Lei %in% ids |  Identifier %in% ids
  )
  found <- ids %in% ids_dict$DtcName | ids %in% ids_dict$Ticker |
    ids %in% ids_dict$Cusip | ids %in% ids_dict$Lei | ids %in% ids_dict$Lei |
    ids %in% ids_dict$Identifier
  if (all(!found)) {
    warning("no ids found")
    return(NULL)
  }
  if (any(!found)) {
    warning(paste0(ids[!found], " not found in msl. "))
    ids_dict <- ids_dict[ix, ]
  }
  is_stock <- ids_dict$SecType == "us-stock" |
    ids_dict$SecType == "intl-stock"
  if (all(!is_stock)) {
    warning("no stock ids found")
    return(NULL)
  }
  if (any(!is_stock)) {
    warning(paste0(ids_dict[!is_stock, ]$DtcName, " is not a stock"))
    ids_dict <- ids_dict[is_stock, ]
  }



}


temp_transfer <- function() {
  ac <- create_arctic(api_keys)
  lib <- get_all_lib(ac)
  xlib <- lib$`co-qual-data`
  sym <- xlib$list_symbols()
  bucket <- create_bucket(api_keys)
  for (i in 1:length(sym)) {
    x <- xlib$read(sym[i])$data
    write_parquet(x, bucket$path(paste0("co-data/", sym[i], ".parquet")))
  }
}





