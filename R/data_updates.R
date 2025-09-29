# ctf daily ----

#' @title Execute Batch API Download from BlackDiamond
#' @description
#' Run before ret_ctf_daily(), the holdings and transactions are needed to
#'   calculate the estimated return
#' @param api_keys list with credentials
#' @param as_of optional parameter for as of date to pull holdings, leave
#'   NULL for last trading day
#' @return does not return data, holdings and transactions are stored in
#'   parquet files in S3
#' @export
hold_ctf_daily <- function(bucket, api_keys, as_of = NULL) {
  if (is.null(as_of)) {
    as_of <- last_us_trading_day()
  }
  api_keys$bd_key <- refresh_bd_key(api_keys, save_local = TRUE)
  id <- download_bd_batch_id(api_keys, as_of)
  Sys.sleep(60)
  res <- download_bd_batch(api_keys, id$id)
  json <- unzip_bd_batch(res)
  handle_bd_batch(bucket, json, as_of)
}

#' @title Update CTF Daily Returns
#' @description
#' Update CTF returns each day from Black Diamond batch data.
#'   The holdings and transactions are needed to create the return estimates.
#'   See hold_ctf_daily().
#' @param bucket S3 FileSystem created by create_bucket()
#' @param date_start first date for return update, defaultt is two trading
#'   days prior to date_end
#' @param date_end lastest date for return update, default is last trading
#'   day
#' @return no data returned, row binds new returns to database "ctf-daily"
#'   record
#' @export
ret_ctf_daily = function(bucket, date_start = NULL, date_end = NULL) {
  if (is.null(date_end)) {
    date_end <- last_us_trading_day()
  }
  if (is.null(date_start)) {
    date_start <- prev_trading_day(date_end, 2)
  }
  ctf <- try_read(bucket, "meta-tables/cust.parquet")
  tbl_msl <- read_msl(bucket)
  ctf <- left_merge(ctf, tbl_msl[, c("DtcName", "Layer")], "DtcName",
                    FALSE)
  ctf <- ctf$union
  sma <- ctf[ctf$Layer < 3, ]
  res <- list()
  # SMA returns from market value and transactions
  for (i in 1:nrow(sma)) {
    fpath <- paste0("transactions/", sma$DtcName[i], ".parquet")
    tx <- try_read(bucket, fpath)
    if (is.null(tx)) {
      next
    }
    tx$TradeDate <- as.Date(tx$TradeDate)
    is_cf <- tx$Action %in% c("Withdrawal", "Contribution")
    if (any(is_cf)) {
      tx$TradeDate[is_cf] <- prev_trading_day(tx$TradeDate[is_cf], 1)
    }
    adj <- group_by(tx, TradeDate) |>
      summarize(Adj = sum(Value))
    adj <- dataframe_to_xts(adj)
    val <- dataframe_to_xts(tx[tx$Action == "EndingValue",
                               c("TradeDate", "Value")])
    price <- xts_cbind(val, adj)
    ret <- price[, "Value"] / lag.xts(price[, "Adj"]) - 1
    colnames(ret) <- sma$DtcName[i]
    res[[i]] <- ret
  }
  new_ret <- do.call(cbind, res)
  new_ret <- cut_time(new_ret, date_start, date_end)
  colnames(new_ret) <- paste0(sapply(res, colnames), " Daily Est.")
  old_ret <- try_read(bucket, "returns/ctf-daily.parquet")
  if (is.null(old_ret)) {
    stop("could not read ctf-daily")
  }
  combo <- xts_rbind(xts_to_dataframe(new_ret), old_ret, is_xts = FALSE,
                     backfill = TRUE)
  try_write(bucket, xts_to_dataframe(combo), "returns/ctf-daily.parquet")
  ctf <- ctf[ctf$Layer == 3, ]
  res <- list()
  # CTF returns, rebalance portfolio
  for (i in 1:nrow(ctf)) {
    tbl_hold <- try_read(bucket, paste0("holdings/", ctf$DtcName[i],
                                        ".parquet"))
    tbl_hold$TimeStamp <- as.Date(tbl_hold$TimeStamp)
    dt_vec <- unique(tbl_hold$TimeStamp)
    ix <- dt_vec >= prev_trading_day(date_start, 1) &
      dt_vec <= next_trading_day(date_end, 1)
    trunc_dt_vec <- dt_vec[ix]
    tbl_hold <- tbl_hold[tbl_hold$TimeStamp %in% trunc_dt_vec, ]
    th <- merge_msl(tbl_hold, tbl_msl)
    tbl_hold <- th$inter
    ix <- tbl_hold$SecType %in% "sma"
    tbl_hold$DtcName[ix] <- paste0(tbl_hold$DtcName[ix], " Daily Est.")
    wgt <- tidyr::pivot_wider(
      data = tbl_hold,
      id_cols = TimeStamp,
      values_from = CapWgt,
      names_from = DtcName
    ) |>
      dataframe_to_xts()
    wgt[is.na(wgt)] <- 0
    r <- read_ret(colnames(wgt), bucket)
    r[is.na(r)] <- 0
    p <- Rebal$new(wgt, r, "D", "D")
    p$rebal()
    res[[i]] <- p$rebal_ret
    colnames(res[[i]]) <- ctf[i, ]$DailyId
  }
  new_ret <- do.call(cbind, res)
  colnames(new_ret) <- sapply(res, colnames)
  new_ret <- cut_time(new_ret, date_start, date_end)
  old_ret <- try_read(bucket, "returns/ctf-daily.parquet")
  if (is.null(old_ret)) {
    stop("could not read ctf-daily")
  }
  combo <- xts_rbind(xts_to_dataframe(new_ret),
                     old_ret, is_xts = FALSE,
                     backfill = TRUE)
  try_write(bucket, xts_to_dataframe(combo), "returns/ctf-daily.parquet")
}

#' @title Adjust daily CTF return estimates to land on month end returns
#' @return does not return data, updates "ctf-daily" record in database
#' @export
ret_ctf_daily_adj = function(bucket) {
  ctf_meta <- try_read(bucket, "meta-tables/ctf-meta.parquet")
  daily <- try_read(bucket, "returns/ctf-daily.parquet")
  if (is.null(daily)) {
    stop("could not load ctf daily returns")
  }
  daily <- dataframe_to_xts(daily)
  monthly <- try_read(bucket, "returns/ctf-monthly.parquet")
  if (is.null(monthly)) {
    stop("could not load ctf monthly returns")
  }
  monthly <- dataframe_to_xts(monthly)
  for (i in 1:ncol(daily)) {
    d <- daily[, i]
    d <- clean_ret(d, eps = 1)
    d <- d$ret
    ix <- colnames(monthly) %in% gsub(" Daily Est.", "", colnames(d))
    if (sum(ix) == 0) {
      warning(paste0(colnames(d), " not found in monthly returns"))
      next
    }
    m <- monthly[, ix]
    res <- try(daily_spline(d, m))
    if (inherits(res, "try-error")) {
      warning(paste0(colnames(d), " did not spline"))
      next
    }
    incpt <- zoo::index(res)[1]
    daily[paste0(incpt, "/"), i] <- res
  }
  try_write(bucket, xts_to_dataframe(daily), "ctf-daily")
}

# mutual funds ----

#' @title Update index returns from Factset
#' @param bucket S3 FileSystem created by create_bucket()
#' @param api_keys list of credentials
#' @param ids leave NULL to update all mutual funds, or enter specific
#'   mutual fund, note if ids are entered they must be in the MSL
#' @param days_back how many days to download
#' @export
ret_mutual_fund = function(bucket, api_keys, ids = NULL, days_back = 1) {
  tbl_msl <- read_msl(bucket)
  if (is.null(ids)) {
    mf <- filter(tbl_msl, ReturnLibrary == "mutual-fund")
    ids <- mf$Ticker
  } else {
    mf <- filter(tbl_msl, Ticker %in% ids)
  }
  formulas <- paste0('P_TOTAL_RETURNC(-', days_back, 'D,NOW,D,USD)')
  iter <- space_ids(ids)
  xdf <- data.frame()
  for (i in 1:(length(iter)-1)) {
    json <- download_fs_formula(
      api_keys = api_keys,
      ids = ids[iter[i]:iter[i+1]],
      formulas = formulas
    )
    xdf <- rob_rbind(xdf, flatten_fs_formula(json))
  }
  colnames(xdf)[2] <- "TotalReturn"
  xdf$TotalReturn <- xdf$TotalReturn / 100
  xdf$DtcName <- mf$DtcName[match(xdf$requestId, mf$Ticker)]
  is_dup <- duplicated(paste0(xdf$date, xdf$DtcName))
  new_dat <- pivot_wider(xdf[!is_dup, ], id_cols = date,
                         names_from = DtcName, values_from = TotalReturn)
  colnames(new_dat)[1] <- "Date"
  old_dat <- try_read(bucket, "returns/mutual-fund.parquet")
  if (is.null(old_dat)) {
    stop("could not read mutual fund old data")
  }
  combo <- xts_rbind(new_dat, old_dat, FALSE, TRUE)
  combo_df <- xts_to_dataframe(combo)
  try_write(bucket, combo_df, "returns/mutual-fund.parquet")
}

# etfs ----

#' @title Update ETF returns from Factset
#' @param bucket S3 FileSystem created by create_bucket()
#' @param api_keys list of credentials
#' @param ids leave `NULL` to udpate all ETFs in the Master Security List,
#'   or enter a vector to only update specific ETFs
#' @param date_start beginning date for update, if left `NULL` will
#'   default to the last date of the existing data
#' @param date_end ending date for update, by default is yesterday
#' @export
ret_etf = function(bucket, api_keys, ids = NULL, date_start = NULL,
                   date_end = NULL) {

  tbl_msl <- read_msl(bucket)
  if (is.null(ids)) {
    etf <- filter(tbl_msl, ReturnLibrary == "etf")
    ids <- etf$Ticker
  } else {
    etf <- filter(tbl_msl, Ticker %in% ids)
  }
  if (is.null(date_end)) {
    date_end <- last_us_trading_day()
  }
  old_dat <- try_read(bucket, "returns/etf.parquet")
  if (is.null(old_dat)) {
    stop("could not read etf old data")
  }
  if (is.null(date_start)) {
    date_start <- prev_trading_day(as.Date(old_dat$Date[nrow(old_dat)]), 2)
  }
  iter <- space_ids(ids)
  xdf <- data.frame()
  for (i in 1:(length(iter)-1)) {
    json <- download_fs_global_prices(
      api_keys = api_keys,
      ids = ids[iter[i]:iter[i+1]],
      date_start = date_start,
      date_end = date_end,
      freq = "D"
    )
    xdf <- rob_rbind(xdf, flatten_fs_global_prices(json))
    print(iter[i])
  }
  xdf$DtcName <- etf$DtcName[match(xdf$RequestId, etf$Ticker)]
  is_dup <- duplicated(paste0(xdf$Date, xdf$DtcName))
  xdf$TotalReturn <- xdf$TotalReturn / 100
  new_dat <- pivot_wider(xdf[!is_dup, ], id_cols = Date,
                         names_from = DtcName, values_from = TotalReturn)
  combo <- xts_rbind(new_dat, old_dat, FALSE)
  combo_df <- xts_to_dataframe(combo)
  try_write(bucket, combo_df, "returns/etf.parquet")
}

# index ----

#' @title Update index returns from Factset
#' @param ids leave NULL to update all indexes, or enter specific index
#'   DtcName to update, note ids will have to first exist in the Master Security
#'   List `tbl_msl`
#' @param t_minus_m how many months to download
#' @export
ret_index = function(bucket, api_keys, ids = NULL, t_minus_m = 1) {
  tbl_msl <- read_msl(bucket)
  if (is.null(ids)) {
    idx <- filter(tbl_msl, ReturnLibrary == "index")
    ids <- idx$Ticker
  } else {
    idx <- filter(tbl_msl, DtcName %in% ids)
    if (nrow(idx) == 0) {
      stop("ids not found, need to specify DtcName(s) in MSL")
    }
    ids <- idx$Ticker
  }
  res <- list()
  is_miss <- rep(FALSE, length(ids))
  for (i in 1:length(ids)) {
    dat <- try(download_fs_ra_ret(ids[i], api_keys, t_minus_m, "D"))
    if ("try-error" %in% class(dat)) {
      is_miss[i] <- TRUE
    } else {
      res[[i]] <- dat
    }
  }
  ret <- do.call("cbind", res)
  dtc_name <- idx$DtcName[!is_miss]
  colnames(ret) <- dtc_name
  old_dat <- try_read(bucket, "returns/index.parquet")
  if (is.null(old_dat)) {
    stop("could not read index old data")
  }
  new_dat <- xts_to_dataframe(ret)
  combo <- xts_rbind(new_dat, old_dat, FALSE)
  combo_df <- xts_to_dataframe(combo)
  try_write(bucket, combo_df, "returns/index.parquet")
}

#' @title Update Private Asset Indexes from Excel
#' @export
ret_private_index = function() {
  base <- "N:/Investment Team/DATABASES/CustomRet/PE-Downloads/"
  pe_q <- read_private_xts(
    paste0(base, "PrivateEquity.xlsx"),
    "Private Equity Index"
  )
  pe_q <- unsmooth_ret(pe_q)
  re_q <- read_private_xts(
    paste0(base, "PrivateRealEstateValueAdd.xlsx"),
    "Private Real Estate Value Add Index"
  )
  re_q <- unsmooth_ret(re_q)
  reg_q <- read_private_xts(
    paste0(base, "PrivateRealEstate.xlsx"),
    "Private Real Estate Index"
  )
  reg_q <- unsmooth_ret(reg_q)
  pc_q <- read_private_xts(
    paste0(base, "PrivateCredit.xlsx"),
    "Private Credit Index"
  )
  pc_q <- unsmooth_ret(pc_q)
  vc_q <- read_private_xts(
    paste0(base, "VentureCapital.xlsx"),
    "Venture Capital Index"
  )
  vc_q <- unsmooth_ret(vc_q)
  ind <- try_read(bucket, "returns/index.parquet")
  ind <- dataframe_to_xts(ind)
  pe_m <- change_freq(na.omit(ind$`Russell 2000`))
  re_m <- change_freq(na.omit(ind$`Wilshire US REIT`))
  pc_m <- change_freq(na.omit(ind$`BofAML U.S. HY Master II`))
  pe <- monthly_spline(pe_m, pe_q)
  re <- monthly_spline(re_m, re_q)
  reg <- monthly_spline(re_m, reg_q)
  pc <- monthly_spline(pc_m, pc_q)
  vc <- monthly_spline(pe_m, vc_q)
  colnames(pe) <- "Private Equity Index"
  colnames(re) <- "Private Real Estate Value Add Index"
  colnames(reg) <- "Private Real Estate Index"
  colnames(pc) <- "Private Credit Index"
  colnames(vc) <- "Private Venture Capital Index"
  dat <- xts_cbind(pe, re)
  dat <- xts_cbind(dat, reg)
  dat <- xts_cbind(dat, pc)
  dat <- xts_cbind(dat, vc)
  xdf <- xts_to_dataframe(dat)
  xdf$Date <- as.character(xdf$Date)
  try_write(bucket, xdf, "returns/private-index.parquet")
}

# ctf monthly ----

#' @title Upload Month End CTF Reconciled Returns into the Database
#' @param ac arcticDB datastore
#' @param xl_path filepath of excel workbook with returns, leave `NULL` for
#'   default
#' @param skip leading header rows to skip, default is `4`
#' @return no data is returned, the entire history is read and stored in
#'   arcticDB
#' @export
ret_ctf_monthly <- function(bucket, xl_path = NULL, skip = 4) {
  tbl_cust <- try_read(bucket, "meta-tables/cust.parquet")
  if (is.null(xl_path)) {
    xl_path <- paste0("N:/Investment Team/DATABASES/FACTSET/",
    "BMO NAV & Platform Return Upload.xlsx")
  }
  dat <- read_xts(xl_path, skip = skip)
  ix <- match(tbl_cust$WorkupId, colnames(dat))
  if (any(is.na(ix))) {
    if (all(is.na(ix))) {
      stop("no values found")
    }
    warning("missing values created when matching workup excel
            to custodian library")
    ix <- na.omit(ix)

  }
  r <- dat[, ix]
  nm_tgt <- match(colnames(r), tbl_cust$WorkupId)
  colnames(r) <- tbl_cust$DtcName[nm_tgt]
  # manually get HFs - need better process here
  tgt <- c(
    "DTC PRIVATE DIVERSIFIERS II COMMON FUND (NoC)",
    "DTC PRIVATE DIVERSIFIERS COMMON FUND (NoC)",
    "DTC SHORT DURATION FIXED INCOME COMMON FUND (NoC)",
    "Magnitude International Class A  (Net)",
    "Turion Onshore, L.P. (Net)",
    "Granville Multi-Strategy Partners, L.P. (Net)",
    "Granville Equity Partners, L.P. (Net)",
    "Winston Global Fund, L.P. (Net)",
    "Winston Hedged Equity Fund, L.P. (Net)"
  )
  x <- dat[, tgt]
  colnames(x)<- c(
    "Private Diversifiers II",
    "Private Diversifiers",
    "Short Duration",
    "Magnitude Capital",
    "Turion",
    "Granville Multi-Strategy",
    "Granville Equity",
    "Winston Global",
    "Winston Hedged Equity"
  )
  r <- xts_cbind(r, x)
  r <- r["1994/"] / 100
  # old_ret <- try_read(bucket, "returns/ctf.parquet")
  # if (is.null(old_ret)) {
  #   stop("could not load ctf old ret")
  # }
  # combo <- xts_rbind(xts_to_dataframe(r),
  #                    old_ret,is_xts = FALSE, backfill = TRUE)
  combo <- xts_to_dataframe(r)
  try_write(bucket, combo, "returns/ctf.parquet")
}

# workup ----

#' @title read in manually uploaded returns from Excel
#' @param xl_file full file path of excel workbook
#' @return does not return data, updates "workup" record in database
#' @export
ret_workup = function(
    xl_file = "N:/Investment Team/DATABASES/CustomRet/workup.xlsx") {

  dat <- read_xts(xl_file)
  dat <- xts_to_dataframe(dat)
  try_write(bucket, dat, "returns/workup.parquet")
}

#' @title read in returns to backfill daily and monthly returns
#' @export
ret_backfill = function() {
  xl_file <- "N:/Investment Team/DATABASES/CustomRet/backfill-daily.xlsx"
  backfill <- read_xts(xl_file)
  backfill <- xts_to_dataframe(backfill)
  try_write(bucket, backfill, "returns/backfill-daily.parquet")
  xl_file <- "N:/Investment Team/DATABASES/CustomRet/backfill-monthly.xlsx"
  backfill <- read_xts(xl_file)
  backfill <- xts_to_dataframe(backfill)
  try_write(bucket, backfill, "returns/backfill-monthly.parquet")
}

#' @title execute backfill
#' @export
run_backfill = function(bucket) {
  ret_meta <- try_read(bucket, "meta-tables/ret-meta.parquet")
  daily <- try_read(bucket, "returns/backfill-daily.parquet")
  if (is.null(daily)) {
    stop("could not read daily backfill")
  }
  tbl_msl <- read_msl(bucket)
  dd <- filter(tbl_msl, DtcName %in% colnames(daily)[-1])
  if (nrow(dd > 0)) {
    ret_lib <- na.omit(unique(dd$ReturnLibrary))
  }
  if (length(ret_lib) > 0) {
    ret_lib <- data.frame(ReturnLibrary = ret_lib)
    res <- left_merge(ret_lib, ret_meta, "ReturnLibrary")
    ret_lib <- res$inter
    if (any(ret_lib$Freq != "daily")) {
      print(ret_lib)
      stop("daily backfill for wrong frequency")
    }
    for (i in 1:nrow(ret_lib)) {
      x <- filter(dd, ReturnLibrary %in% ret_lib$ReturnLibrary[i])
      fpath <- paste0("returns/", ret_lib$ReturnLibrary[i], ".parquet")
      new <- try_read(bucket, fpath)
      if (is.null(new)) {
        stop(paste0("could not read ", fpath))
      }
      combo <- xts_rbind(new, daily[, c("Date", x$DtcName)], FALSE, TRUE)
      combo <- xts_to_dataframe(combo)
      try_write(bucket, combo, fpath)
    }
  }
  monthly <- try_read(bucket, "returns/backfill-monthly.parquet")
  if (is.null(monthly)) {
    stop("could not read monthly backfill")
  }
  dd <- filter(tbl_msl, DtcName %in% colnames(monthly)[-1])
  if (nrow(dd > 0)) {
    ret_lib <- na.omit(unique(dd$ReturnLibrary))
  }
  if (length(ret_lib) > 0) {
    ret_lib <- data.frame(ReturnLibrary = ret_lib)
    res <- left_merge(ret_lib, ret_meta, "ReturnLibrary")
    ret_lib <- res$inter
    if (any(ret_lib$Freq != "monthly")) {
      print(ret_lib)
      stop("monthly backfill for wrong frequency")
    }
    for (i in 1:nrow(ret_lib)) {
      x <- filter(dd, ReturnLibrary %in% ret_lib$ReturnLibrary[i])
      fpath <- paste0("returns/", ret_lib$ReturnLibrary[i], ".parquet")
      new <- try_read(bucket, fpath)
      if (is.null(new)) {
        stop(paste0("could not read ", fpath))
      }
      combo <- xts_rbind(new, monthly[, c("Date", x$DtcName)], FALSE, TRUE)
      combo <- xts_to_dataframe(combo)
      try_write(bucket, combo, fpath)
    }
  }
}

# model ----

#' @title Update returns of models
#' @param dtc_name option to specify specific models to update, leave NULL
#'   to update all models
#' @param months_back integer representing how many months back to update
#'   returns for, default is 1
#' @export
ret_model = function(bucket, api_keys, dtc_name = NULL, months_back = 1) {
  msl <- read_msl(bucket)
  model <- try_read(bucket, "meta-tables/model.parquet")
  if (!is.null(dtc_name)) {
    model <- filter(model, DtcName %in% dtc_name)
  }
  d <- filter(model, ReturnLibrary == "model-daily")
  d_id <- filter(msl, DtcName %in% d$DtcName)
  if (nrow(d_id) > 0) {
    dat <- list()
    found <- rep(TRUE, nrow(d_id))
    for (i in 1:nrow(d_id)) {
      x <- try(download_fs_ra_ret(d_id$Identifier[i], api_keys,
                                  months_back))
      if ("try-error" %in% class(x)) {
        found[i] <- FALSE
      } else {
        dat[[i]] <- x
      }
    }
    r <- do.call(cbind, dat)
    colnames(r) <- d_id$DtcName[found]
    new <- xts_to_dataframe(r)
    old <- try_read(bucket, "returns/model-daily.parquet")
    if (is.null(old)) {
      stop("could not read old daily model returns")
    }
    combo <- xts_rbind(new, old, FALSE, TRUE)
    try_write(bucket, xts_to_dataframe(combo), "returns/model-daily.parquet")
  }
  m <- filter(model, ReturnLibrary == "model-monthly")
  m_id <- filter(msl, DtcName %in% m$DtcName)
  if (nrow(m_id) > 0) {
    dat <- list()
    found <- rep(TRUE, nrow(m_id))
    for (i in 1:nrow(m_id)) {
      x <- try(download_fs_ra_ret(m_id$Identifier[i], api_keys,
                                  months_back, "M"))
      if ("try-error" %in% class(x)) {
        found[i] <- FALSE
      } else {
        dat[[i]] <- x
      }
    }
    r <- do.call(cbind, dat)
    colnames(r) <- m_id$DtcName[found]
    new <- xts_to_dataframe(r)
    old <- try_read(bucket, "returns/model-monthly.parquet")
    if (is.null(old)) {
      stop("could not read monthly model old returns")
    }
    combo <- xts_rbind(new, old, FALSE, TRUE)
    try_write(bucket, xts_to_dataframe(combo), "returns/model-monthly.parquet")
  }
}

# stocks----

#' @title Update stock returns
#' @param ids optional parameter to only update certain stocks, leave NULL
#'   to update all stocks in the MSL
#' @param date_start starting date for new returns, default is NULL which
#'   will find most recent date of existing returns to start
#' @param date_end most recent date for new returns, default is Sys.Date()
#' @param freq "D" for daily
#' @param geo "us" for US Stocks or "intl" for international stocks
#' @return does not return data, either updates "us-stock" or "intl-stock"
#'   records in database
#' @seealso \code{\link{download_fs_global_prices}}
#' @export
ret_stock = function(bucket, api_keys,
                     ids = NULL, date_start = NULL, date_end = NULL,
                     freq = "D", geo = c("us", "intl")) {

  tbl_msl <- read_msl(bucket)
  geo <- tolower(geo[1])
  if (!geo %in% c("us", "intl")) {
    stop("geo must be us or intl")
  }
  geo <- paste0(geo, "-stock")
  if (is.null(ids)) {
    stock <- filter(tbl_msl, ReturnLibrary == geo)
    ids <- create_ids(stock)
  } else {
    ix <- match_ids_dtc_name(ids, tbl_msl)
    if (any(is.na(ix))) {
      stop("ids missing from MSL")
    }
  }
  fpath <- paste0("returns/", geo, ".parquet")
  old_dat <- try_read(bucket, fpath)
  if (is.null(old_dat)) {
    stop("could not read old stock returns")
  }
  if (is.null(date_start)) {
    date_start <- old_dat$Date[nrow(old_dat)]
    date_start <- prev_trading_day(as.Date(date_start), 1)
  }
  if (is.null(date_end)) {
    date_end <- last_us_trading_day()
  }
  iter <- space_ids(ids)
  xdf <- data.frame()
  for (i in 1:(length(iter)-1)) {
    json <- download_fs_global_prices(
      api_keys = api_keys,
      ids = ids[iter[i]:iter[i+1]],
      date_start = date_start,
      date_end = date_end,
      freq = freq
    )
    xdf <- rob_rbind(xdf, flatten_fs_global_prices(json))
    print(iter[i])
    Sys.sleep(1)
  }
  ix <- match_ids_dtc_name(xdf$RequestId, tbl_msl)
  dtc_name <- tbl_msl$DtcName[ix]
  xdf$DtcName <- dtc_name
  is_dup <- duplicated(paste0(xdf$DtcName, xdf$Date))
  xdf <- xdf[!is_dup, ]
  xdf$TotalReturn <- xdf$TotalReturn / 100
  new_dat <- pivot_wider(xdf, id_cols = Date, values_from = TotalReturn,
                         names_from = DtcName)
  combo <- xts_rbind(new_dat, old_dat, FALSE, TRUE)
  try_write(bucket, xts_to_dataframe(combo), fpath)
}

# hfr index ----

#' @title Update HFRI Return index from csv file
#' @param file_nm full file name of csv file
#' @seealso \code{\link{read_hfr_index}}
#' @return does not return data, updates "hfr-index" record in the database
ret_hfr_index = function(file_nm) {
  dat <- read_hfr_csv(file_nm)
  dat <- dat / 100
  try_write(bucket, xts_to_dataframe(dat), "returns/hfr-index.parquet")
}

# cash plus x ----

#' @title Update returns that require computational changes
#' @details
#' for example cash plus 200 bps reads the returns of cash, adds 200 bps,
#' and then saves as a new data-field (return column)
#' @return does not return data, updates various return records in the
#'   database
#' @export
ret_comp = function() {
  # cash plus 200 and 400 bps
  cash <- read_ret("BofAML U.S. Treasury Bill 3M", bucket)
  cash_plus_2 <- cash + 0.02 / 252
  cash_plus_4 <- cash + 0.04 / 252
  rec <- try_read(bucket, "returns/index.parquet")
  if (is.null(rec)) {
    stop("could not load index returns for cash plus")
  }
  rec[, "Cash Plus 200 bps"] <- as.vector(cash_plus_2)
  rec[, "Cash Plus 400 bps"] <- as.vector(cash_plus_4)
  try_write(bucket, rec, "returns/index.parquet")
  # money market proxy with cash
  mmkt <- cash
  colnames(mmkt) <- "Blackrock Liquidity Fed Funds (TFDXX)"
  try_write(bucket, xts_to_dataframe(mmkt), "returns/money-market.parquet")
}

# fred ----

#' @title Update econ time-series from St. Louis FED (FRED)
#' @return does not return data, updates "fred-monthly" record in database
ret_fred = function(bucket) {
  tbl_msl <- read_msl(bucket)
  dict <- filter(tbl_msl, ReturnLibrary == "fred-monthly")
  fred <- try_read(bucket, "meta-tables/fred-meta.parquet")
  x <- left_merge(dict, fred$data, match_by = "DtcName")
  if (nrow(x$miss) > 0) {
    warning(x$miss$DtcName)
  }
  if (nrow(x$inter) == 0) {
    stop("no fred records found")
  }
  dat <- list()
  for (i in 1:nrow(x$inter)) {
    dat[[i]] <- download_fred(x$inter$Ticker[i], self$api_keys$fred)
    if (tolower(x$inter$Type[i]) == "price") {
      dat[[i]] <- price_to_ret(dat[[i]])
    }
  }
  r <- do.call(cbind.xts, dat)
  tix <- sapply(dat, colnames)
  ix <- match_ids_dtc_name(tix, self$tbl_msl)
  dtc_name <- self$tbl_msl$DtcName[ix]
  colnames(r) <- dtc_name
  r <- xts_eo_month(r)
  try_write(bucket, xts_to_dataframe(r), "returns/fred-monthly.parquet")
}

# holdings ----

#' @title Download holdings from SEC EDGAR Database
#' @param dtc_name leave `NULL` to download all, or enter a vector of
#'   dtc_names to download specific funds
#' @param user_email need to provide an email address to download
#' @param save_to_db boolean to write to DTC's database, default is TRUE
#' @param return_data boolean to return data.frame of holdings, default is FALSE
#' @export
hold_sec <- function(bucket, dtc_name = NULL,
                     user_email = "asotolongo@diversifiedtrust.com",
                     save_to_db = TRUE, return_data = FALSE) {

  sec <- try_read(bucket, "meta-tables/sec.parquet")
  if (!is.null(dtc_name)) {
    sec <- filter(sec, DtcName %in% dtc_name)
    if (nrow(sec) == 0) {
      stop("dtc_names not found in SEC table")
    }
  } else {
    dtc_name <- sec$DtcName
  }
  res <- list()
  for (i in 1:length(dtc_name)) {
    print(paste0("working on ", dtc_name[i]))
    dat <- try(download_sec(sec$LongCIK[i], sec$ShortCIK[i], user_email))
    if ("try-error" %in% class(dat)) {
      warning(paste0("could not download ", dtc_name[i]))
      next
    }
    if (save_to_db) {
      if (paste0("holdings/", dtc_name[i], ".parquet") %in%
          bucket$ls("holdings/")) {
        old_dat <- try_read(bucket, paste0("holdings/", dtc_name[i],
                                           ".parquet"))
        if (is.null(old_dat)) {
          warning("could not read old data")
          next
        }
        dup_date <- dat$TimeStamp[1] %in%
          unique(old_dat$TimeStamp)
        if (dup_date) {
          warning(paste0(dtc_name[i], " already has data for latest date"))
          if (return_data) {
            res[[i]] <- dat
          }
          next
        }
        combo <- rob_rbind(old_dat, dat)
        try_write(bucket, combo, paste0("holdings/", dtc_name[i], ".parquet"))
      } else {
        warning(
          paste0(
            dtc_name[i],
            " not found in library, creating new symbol"
          )
        )
        try_write(bucket, dat, paste0("holdings/", dtc_name[i], ".parquet"))
      }
    }
    if (return_data) {
      res[[i]] <- dat
    }
  }
  if (return_data) {
    return(res)
  }
}

# fundamental data

#' @title Download equity financial data
#' @param api_keys list with api keys
#' @param bucket s3 filesystem
#' @param ids leave `NULL` to get for all stocks, or enter specific ids
#' @param yrs_back how many years of data to pull
#' @param dtype data type: one of PE, PB, PFCF, DY, ROE, and MCAP
#' @return does not return data, updates database with fundamental data
#' @seealso \code{\link{download_fs_formula}}
#' @export
co_fundamental_data = function(api_keys, bucket, ids = NULL, yrs_back = 1,
    dtype = c('PE', 'PB', 'PFCF', 'DY', 'ROE', 'MCAP')) {

  dtype <- dtype[1]
  if (dtype == 'PE') {
    formulas <- paste0('FG_PE(-', yrs_back, 'AY,NOW,CQ)')
  } else if (dtype == 'PB') {
    formulas <- paste0('FG_PBK(-', yrs_back, 'AY,NOW,CQ)')
  } else if (dtype == 'PFCF') {
    formulas <- paste0('FG_CFLOW_FREE_EQ_PS(-', yrs_back, 'AY,NOW,CQ,USD)')
  } else if (dtype == 'DY') {
    formulas <- paste0('FG_DIV_YLD(-', yrs_back, 'AY,NOW,CQ)')
  } else if (dtype == 'ROE') {
    formulas <- paste0('FG_ROE(-', yrs_back, 'AY,NOW,CQ)')
  } else if (dtype == 'MCAP') {
    formulas <- paste0('FF_MKT_VAL(ANN_R,-', yrs_back, 'AY,NOW,CQ,,USD)')
  } else {
    stop("dtype must be 'PE', 'PB', 'PFCF', 'DY', 'ROE', or 'MCAP'")
  }
  tbl_msl <-read_msl(bucket)
  if (is.null(ids)) {
    stock <- filter(tbl_msl, SecType == "us-stock" | SecType == "intl-stock")
    ids <- create_ids(stock)
  } else {
    ix <- match_ids_dtc_name(ids, tbl_msl)
    if (any(is.na(ix))) {
      stop("ids missing from MSL")
    }
  }
  ids <- gsub(" ", "", ids)
  iter <- space_ids(ids)
  xdf <- data.frame()
  for (i in 1:(length(iter)-1)) {
    json <- download_fs_formula(api_keys, ids[iter[i]:iter[i+1]],
                                formulas)
    xdf <- rbind(xdf, flatten_fs_formula(json))
    print(iter[i])
  }
  ix <- match_ids_dtc_name(xdf$requestId, tbl_msl)
  dtc_name <- tbl_msl$DtcName[ix]
  xdf$DtcName <- dtc_name
  is_dup <- duplicated(paste0(xdf$DtcName, xdf$date))
  xdf <- xdf[!is_dup, ]
  colnames(xdf)[2:3] <- c(dtype, "Date")
  wdf <- pivot_wider(xdf, id_cols = Date, names_from = DtcName,
                     values_from = all_of(dtype))
  old_data <- try_read(bucket, paste0("co-data/", dtype, ".parquet"))
  if (is.null(old_data)) {
    stop("could not read old data")
  }
  combo <- xts_rbind(wdf, old_data, FALSE)
  combo_df <- xts_to_dataframe(combo)
  try_write(bucket, combo_df, paste0("co-data/", dtype, ".parquet"))
}

#' @export
download_sectors = function(bucket, api_keys, ids = NULL) {
  tbl_msl <- read_msl(bucket)
  if (is.null(ids)) {
    stock <- filter(tbl_msl, SecType == "us-stock" |
                      SecType == "intl-stock")
    ids <- stock$Isin
  } else {
    ix <- match_ids_dtc_name(ids, tbl_msl)
    if (any(is.na(ix))) {
      stop("ids missing from MSL")
    }
  }
  ids[is.na(ids)] <- stock$Cusip[is.na(ids)]
  ids <- gsub(" ", "", ids)
  ids <- na.omit(ids)
  iter <- space_ids(ids)
  xformula <- "FG_FACTSET_SECTOR"
  xdf <- data.frame()
  for (i in 1:(length(iter)-1)) {
    json <- download_fs_formula(
      api_keys = api_keys,
      ids = ids[iter[i]:iter[i+1]],
      formulas = xformula
    )
    xdf <- rbind(xdf, flatten_fs_formula(json))
  }
  colnames(xdf) <- c("RequestId", "FactsetSector")
  is_dup <- duplicated(xdf$RequestId)
  xdf <- xdf[!is_dup, ]
  ix <- match(xdf$RequestId, stock$Isin)
  ix[is.na(ix)] <- match(xdf$RequestId, stock$Cusip)[is.na(ix)]
  xdf$DtcName <- stock$DtcName[ix]
  r3 <- try_read(bucket, "co-data/macro_sel_r3.parquet")
  # TO-DO add ACWI
  r3 <- rename(r3, Isin = ISIN)
  res <- left_merge(r3, tbl_msl, c("Ticker", "Isin"))
  res <- left_merge(xdf, res$inter, "DtcName")
  sect <- res$union[, c("RequestId", "FactsetSector", "DtcName", "Sector")]
  sect <- rename(sect, GicsMacro = Sector)
  sect_map <- try_read(bucket, "meta-tables/sector-map.parquet")
  res <- left_merge(sect, sect_map, c("FactsetSector"))
  try_write(bucket, res$union, "co-data/sector.parquet")
}


#' @title Download Macro Select Workbook and Save to Library
#' @param bucket s3 file system
#' @param wb file location of workbook
#' @param is_us TRUE for Russell 3000, FALSE for MSCI ACWI
#' @export
update_ps_macro_select = function(bucket, wb, is_us = TRUE) {
  if (is_us) {
    idx_nm <- "Russell 3000"
  } else {
    idx_nm <- "MSCI ACWI"
  }
  dat <- read_macro_wb(wb, idx_nm)
  bad_row <- rowSums(is.na(dat)) == ncol(dat)
  dat <- dat[!bad_row, ]
  if (is_us) {
    try_write(bucket, dat, "co-data/macro_sel_r3.parquet")
  } else {
    try_write(bucket, dat, "co-data/macro_sel_acwi.parquet")
  }
}

#' @export
piper_sandler_macro <- function(bucket, wb, idx_nm = "Russell 3000",
                                fct_nm = NULL) {
  dat <- read_macro_wb(wb, idx_nm)
  if (is.null(fct_nm)) {
    colnames(dat)[8:11] <- paste0("MacroSelect", 1:4)
  } else {
    colnames(dat)[8:11] <- fct_nm
  }
  colnames(dat)[12] <- "MacroSelectRank"
  tbl_msl <- read_msl(bucket)
  res <- merge_msl(dat, tbl_msl)
  if (idx_nm == "Russell 3000") {
    try_write(bucket, res$inter, "co-data/macro_sel_r3.parquet")
  }
  if (idx_nm == "ACWI") {
    try_write(bucket, res$inter, "co-data/macro_sel_acwi.parquet")
  }
}




#' @export
latest_fina <- function(bucket) {
  pe <- try_read(bucket, "co-data/PE.parquet")
  pb <- try_read(bucket, "co-data/PB.parquet")
  pfcf <- try_read(bucket, "co-data/PFCF.parquet")
  dy <- try_read(bucket, "co-data/DY.parquet")

  pe <- data.frame(DtcName = colnames(pe)[-1],
                   PE = as.numeric(pe[nrow(pe), -1]))

  pb <- data.frame(DtcName = colnames(pb)[-1],
                   PB = as.numeric(pb[nrow(pb), -1]))

  pfcf <- data.frame(DtcName = colnames(pfcf)[-1],
                     PFCF = as.numeric(pfcf[nrow(pfcf), -1]))

  dy <- data.frame(DtcName = colnames(dy)[-1],
                   DY = as.numeric(dy[nrow(dy), -1]))

  xdf <- left_merge(pe, pb, "DtcName")
  xdf <- left_merge(xdf$union, pfcf, "DtcName")
  xdf <- left_merge(xdf$union, dy, "DtcName")
  return(xdf$union)
}

rm_dup_holdings <- function(bucket) {
  # be careful, will remove duplicates based on Name field, need to subset
  # SEC and BD, really subset BD
  files <- bucket$ls("holdings/")
  for (i in 1:length(files)) {
    x <- try_read(bucket, files[i])
    x$TimeStamp <- force_date(x$TimeStamp)
    x <- remove_dup_dates(x)
    try_write(bucket, x, files[i])
  }
}
