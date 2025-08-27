# ctf daily ----

#' @export
hold_ctf_daily <- function(api_keys, as_of = NULL) {
  if (is.null(as_of)) {
    as_of <- last_us_trading_day()
  }
  api_keys$bd_key <- refresh_bd_key(api_keys$bd_key, save_local = TRUE)
  id <- download_bd_batch_id(api_keys, as_of)
  Sys.sleep(120)
  res <- download_bd_batch(api_keys, id$id)
  json <- unzip_bd_batch(res)
  handle_bd_batch(bucket, json, as_of)
}

#' @title Update CTF Daily Returns
#' @description
#' Update CTF returns each day from Black Diamond bulk data
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
    ix <- tbl_hold$SecType %in% c("sma")
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

# mutual funds ----

#' @title Update index returns from Factset
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
