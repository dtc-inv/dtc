#' @title Download price time-series from Tiingo
#' @param ticker character of stock ticker to download, see
#'   `download_tiingo_tickers` for downloading multiple tickers
#' @param t_api API key
#' @param date_start inception for time-series
#' @param date_end last day of time-series, if left `NULL` will default to today
#' @return `data.frame` of historical prices (adjusted for splits and dividends)
#'   with dates in first column
#' @export
download_tiingo_csv <- function(ticker, t_api, date_start = '1970-01-01',
                                date_end = NULL) {
  if (is.null(date_end)) {
    date_end <- Sys.Date()
  }
  t_url <- paste0('https://api.tiingo.com/tiingo/daily/',
                  ticker,
                  '/prices?startDate=', date_start,
                  '&endDate=', date_end,
                  '&format=csv&resampleFreq=daily',
                  '&token=', t_api)
  dat <- try(read.csv(t_url), silent = TRUE)
  if ('try-error' %in% class(dat)) {
    warning(paste0(ticker, ' was not downloaded'))
    return(NULL)
  }
  print(paste0(ticker, ' downloaded'))
  dat <- try(dat[, c('date', 'adjClose')], silent = TRUE)
  if ('try-error' %in% class(dat)) {
    warning(paste0(ticker, ' columns not found'))
    return(NULL)
  }
  colnames(dat)[2] <- ticker
  return(dat)
}


#' @title Download multiple tickers from Tiingo
#' @param ticker_vec character vector of tickers to download
#' @param t_api API key
#' @param date_start first date in time-series to download, need to be Date class,
#'   e.g., as.Date('2024-01-01')
#' @param date_end last date in time-series to download, if left `NULL` will
#'   default to today
#' @param out_ret boolean if TRUE outputs returns, if FALSE outputs prices
#' @param out_xts boolean, if TRUE outputs xts object, if FALSE returns data.frame
#' @return xts of price time-series adjusted for dividends and splits
#' @export
download_tiingo_tickers <- function(ticker_vec, t_api, date_start = NULL,
                                    date_end = NULL, out_ret = FALSE,
                                    out_xts = FALSE) {

  if (is.null(date_start)) {
    date_start <- as.Date('1970-01-01')
  }
  utick <- unique(ticker_vec)
  if (length(utick) < length(ticker_vec)) {
    warning('duplicated tickers found and removed')
  }
  dat <- lapply(utick, download_tiingo_csv, t_api = t_api,
                date_start = date_start, date_end = date_end)
  dat <- dat[!sapply(dat, is.null)]
  if (length(dat) == 0) {
    warning('no tickers found')
    return()
  }
  nm <- sapply(dat, function(x) {colnames(x)[2]})
  if (length(dat) == 1) {
    price <- dat[[1]][, 2]
    dt <- dat[[1]][, 1]
  } else {
    dt <- us_trading_days(date_start, date_end)
    price <- matrix(nrow = length(dt), ncol = length(dat))
    for (i in 1:ncol(price)) {
      ix <- match(dat[[i]]$date, dt)
      price_on_holiday <- is.na(ix)
      price[na.omit(ix), i] <- dat[[i]][[2]][!price_on_holiday]
      if (i %% 100 == 0) print(paste0(i, ' out of ', length(dat)))
    }
  }
  price_df <- data.frame('date' = dt, price)
  colnames(price_df) <- c('date', nm)
  if (out_ret) {
    res <- mat_to_xts(price_df)
    res <- price_to_ret(res)
    colnames(res) <- nm
    if (!out_xts) {
      res <- xts_to_dataframe(res)
    }
  } else {
    res <- price_df
    if (out_xts) {
      res <- mat_to_xts(price_df)
      colnames(res) <- nm
    }
  }
  return(res)
}

# fred ----

#' @title Download Econ Time-series from FED Database
#' @param series_id character to represent the series you want to download
#' @param fred_api API key, see https://fred.stlouisfed.org/ to create one
#' @return xts of economic time-series
#' @export
download_fred <- function(series_id, fred_api) {
  f_url <- paste0('https://api.stlouisfed.org/fred/series/observations?series_id=',
                  series_id, '&api_key=', fred_api, '&file_type=json')
  request <- httr::GET(f_url)
  dat <- jsonlite::parse_json(request)
  dt <- sapply(dat$observations, '[[', 'date')
  obs <- sapply(dat$observations, '[[', 'value')
  x <- xts(as.numeric(obs), as.Date(dt))
  colnames(x) <- series_id
  return(x)
}

# BlackDiamond ----

#' @title Refresh Black Diamond Token for API Authorization
#' @param api_keys list of API Keys
#' @param save_to_n if on DTC network option to save to N: drive
#' @param save_local save to local directory, e.g., "~/"
#' @param return returns the bd section of the api_keys (api_keys$bd_key)
#' @export
refresh_bd_key <- function(api_keys, save_to_n = FALSE, save_local = FALSE) {
  bd_key <- api_keys$bd_key
  response <- httr::POST('https://login.bdreporting.com/connect/token',
                         encode = 'form',
                         body = list(grant_type = 'password',
                                     client_id = bd_key$client_id,
                                     client_secret = bd_key$client_secret,
                                     username = bd_key$username,
                                     password = bd_key$password))
  tk <- jsonlite::parse_json(response)
  bd_key$refresh_token <- tk$access_token
  if (save_to_n) {
    api_keys$bd_key <- bd_key
    save(api_keys,
         file = 'N:/Investment Team/DATABASES/CustomRet/keys/api_keys.RData')
  }
  if (save_local) {
    api_keys$bd_key <- bd_key
    save(api_keys, file = "~/api_keys.RData")
  }
  return(bd_key)
}

#' @title Get Batch ID for bulk download
#' @param api_keys list of API Keys
#' @param as_of optional date, defaults to last trading day, transactions
#'   will default to last 7 days
#' @return batch id to pass to \code{\link{download_bd_batch}} for bulk download
#' @export
download_bd_batch_id <- function(api_keys, as_of = NULL) {
  if (is.null(as_of)) {
    as_of <- last_us_trading_day()
  }
  bd_key <- api_keys$bd_key
  body <- list(returnDate = format(as_of, "%m-%d-%Y"),
               batchFiles = list("accounts"),
               includeTransactions = TRUE, includeHoldings = TRUE)
  response <- httr::POST(
    'https://api.blackdiamondwealthplatform.com/batchV5',
    accept_json(),
    add_headers(
      Authorization = paste0('Bearer ', bd_key$refresh_token),
      `Ocp-Apim-Subscription-Key` = bd_key$bd_subkey
    ),
    encode = 'json',
    body = body)
  if (response$status_code != 200) {
    bd_key <- refresh_bd_key(api_keys, FALSE, TRUE)
    response <- httr::POST(
      'https://api.blackdiamondwealthplatform.com/batchV5',
      accept_json(),
      add_headers(
        Authorization = paste0('Bearer ', bd_key$refresh_token),
        `Ocp-Apim-Subscription-Key` = bd_key$bd_subkey
      ),
      encode = 'json',
      body = body)
  }
  res <- parse_json(response)
  return(res)
}

#' @title BlackDiamond Batch Download
#' @param api_keys list of API Keys
#' @param batch_id from \code{\link{download_bd_batch_id}}
#' @return response with zipped raw data
#' @seealso \code{\link{unzip_bd_batch}} to read the response output and
#' \code{\link{download_bd_batch_id}} for the
#'   \code{batch_id} input
#' @export
download_bd_batch <- function(api_keys, batch_id) {
  bd_key <- api_keys$bd_key
  url <- paste0(
    "https://api.blackdiamondwealthplatform.com/batch/",
    batch_id
  )
  resp <- httr::GET(
    url,
    accept_json(),
    add_headers(
      Authorization = paste0('Bearer ', bd_key$refresh_token),
      `Ocp-Apim-Subscription-Key` = bd_key$bd_subkey
    ),
    encode = 'json'
  )
  return(resp)
}

#' @title Unzip Response from BlackDiamond Batch Download
#' @param resp response returned by \code{\link{download_bd_batch}}
#' @return bulk data from the json download nested in a list
#' @seealso \code{\link{download_bd_batch}} for downloading the bulk data
#' @export
unzip_bd_batch <- function(resp) {
  tmp <- tempfile(fileext = ".zip")
  brio::write_file_raw(resp$content, path = tmp)
  ufile <- unzip(tmp, exdir = "~/json/")
  if (is.null(ufile)) {
    save(resp, file = "~/resp.RData")
    stop("error in unzipping, saving ~/resp.RData")
  }
  read_json(ufile)
}

#' @title Parse JSON file from BlackDiamond Batch Download Process
#' @param ac arcticDB datastore
#' @param json data read in from \code{\link{unzip_bd_batch}}
#' @return does not return data, transactions, market values, and holdings
#'   are stored in arcticDB cloud (S3) through \code{ac}
#' @seealso \code{\link{unzip_bd_batch}} and \code{\link{download_bd_batch}}
#'   for downloading bulk data from black diamond and \code{\link{create_artic}}
#'   for arcticDB datastore
#' @export
handle_bd_batch <- function(bucket, json, as_of) {
  cust <- try_read(bucket, "meta-tables/cust.parquet")
  as_of <- as.character(as_of)
  for (i in 1:length(json)) {
    x <- json[[i]]
    if (!x$Id %in% cust$BdAccountId) {
      next
    }
    print(x$Name)
    ix <- match(x$Id, cust$BdAccountId)
    dtc_name <- cust$DtcName[ix]
    mv <- x$Details$TotalEmv
    if (is.null(mv)) {
      mv <- NA
    }
    cf_dat <- data.frame(
      DtcName = dtc_name,
      TradeDate = as_of,
      Action = "EndingValue",
      Value = mv,
      TransactionId = paste0("EndingValue", "-", as_of)
    )
    cf_type <- sapply(x$Transactions, "[[", "ExternalFlowAffect")
    is_cf <- cf_type != "NotExternalFlow"
    if (any(is_cf)) {
      cf <- x$Transactions[is_cf]
      for (j in 1:length(cf)) {
        dir <- extract_list_null(cf[[j]], "ExternalFlowAffect")
        if (dir == "Withdrawal") {
          cf_dat[j + 1, "TradeDate"] <- extract_list_null(cf[[j]], "TradeDate")
          cf_dat[j + 1, "Action"] <- dir
          cf_dat[j + 1, "Value"] <- -extract_list_null(cf[[j]], "MarketValue")
          cf_dat[j + 1, "TransactionId"] <- extract_list_null(cf[[j]], "TransactionID")
        } else if (dir == "Contribution") {
          cf_dat[j + 1, "TradeDate"] <- extract_list_null(cf[[j]], "TradeDate")
          cf_dat[j + 1, "Action"] <- dir
          cf_dat[j + 1, "Value"] <- extract_list_null(cf[[j]], "MarketValue")
          cf_dat[j + 1, "TransactionId"] <- extract_list_null(cf[[j]], "TransactionID")
        } else {
          warning(paste0(dtc_name, " unfamiliar transaction"))
        } # end if dir
      } # end cf loop
    } # end any cf
    cf_dat$DtcName <- dtc_name
    fpath <- paste0("transactions/", dtc_name, ".parquet")
    old_dat <- try_read(bucket, fpath)
    if (is.null(old_dat)) {
      warning(paste0("no old data for ", dtc_name))
      next
    }
    combo <- rbind_tx(old_dat, cf_dat)
    try_write(bucket, combo, fpath)
    hold <- x$Holdings
    fld <- sapply(hold, names) |> unlist() |> unique()
    xdf <- data.frame(x = rep(NA, length(hold)))
    for (h in 1:length(fld)) {
      dat <- sapply(hold, "[[", fld[h])
      dat <- list_replace_null(dat) |> unlist()
      if (is.null(dat)) {
        dat <- rep(NA, length(hold))
      }
      xdf[, h] <- dat
    }
    if ("AssetName" %in% fld) {
      fld[fld %in% "AssetName"] <- "Name"
    }
    if ("AlternateId" %in% fld) {
      fld[fld %in% "AlternateId"] <- "Identifier"
    }
    if ("MarketValue" %in% fld) {
      fld[fld %in% "MarketValue"] <- "Value"
    }
    if ("AsOfDate" %in% fld) {
      fld[fld %in% "AsOfDate"] <- "TimeStamp"
    }
    colnames(xdf) <- fld
    xdf$CapWgt <- xdf$Value / sum(xdf$Value, na.rm = TRUE)
    fpath <- paste0("holdings/", dtc_name, ".parquet")
    old_dat <- try_read(bucket, fpath)
    if (is.null(old_dat)) {
      warning(paste0("could not read existing data: ", dtc_name))
      warning("create a blank data.frame for new investments")
      next
    }
    combo <- rob_rbind(old_dat, xdf)
    out <- try_write(bucket, combo, fpath)
    if (is.null(out)) {
      warning(paste0("could not write: ", dtc_name))
    }
  } # end json loop
}

#' @title Utility to extract field from list checking for NULL
#' @param x list
#' @param nm field (string)
#' @return NA if field is NULL or field value if not NULL
#' @export
extract_list_null <- function(x, nm) {
  if (is.null(x[nm][[1]])) {
    return(NA)
  } else {
    return(x[nm][[1]])
  }
}

#' @title Utility to row bind transaction data
#' @param old existing data.frame of transactions
#' @param new newly downloaded transactions
#' @return combo of new and old, removes duplicated values for new from old
#' @seealso used as part of \code{\link{handle_bd_batch}}
#' @export
rbind_tx <- function(old, new) {
  ix <- na.omit(match(new$TransactionId, old$TransactionId))
  if (length(ix) > 0) {
    old <- old[-ix, ]
  }
  rbind(old, new)
}

#' @title Download Holdings from a Black Diamond Account
#' @param account_id external account ID from Black Diamond
#' @param api_keys list of API Keys
#' @param as_of as of date for holdings, leave \code{NULL} to default to
#'   last NYSE trading day
#' @return \code{data.frame} with holdings name, ticker, cusip, and value
#' @export
download_bd <- function(account_id, api_keys, as_of = NULL) {
  if (is.null(as_of)) {
    as_of <- last_us_trading_day()
  }
  bd_key <- api_keys$bd_key
  response <- httr::POST(
    'https://api.blackdiamondwealthplatform.com/account/Query/HoldingDetailSearch',
    accept_json(),
    add_headers(
      Authorization = paste0('Bearer ', bd_key$refresh_token),
      `Ocp-Apim-Subscription-Key` = bd_key$bd_subkey
    ),
    encode = 'json',
    body = list(accountID = account_id, asOf = as_of,
                onlyCurrentHoldings = TRUE, limit = 100000,
                include = list(returnInfo = TRUE, assets = TRUE))
  ) # end POST
  if (response$status_code != 200) {
    bd_key <- refresh_bd_key(api_keys, FALSE, TRUE)
    response <- httr::POST(
      'https://api.blackdiamondwealthplatform.com/account/Query/HoldingDetailSearch',
      accept_json(),
      add_headers(
        Authorization = paste0('Bearer ', bd_key$refresh_token),
        `Ocp-Apim-Subscription-Key` = bd_key$bd_subkey
      ),
      encode = 'json',
      body = list(accountID = account_id, asOf = as_of,
                  onlyCurrentHoldings = TRUE, limit = 100000,
                  include = list(returnInfo = TRUE, assets = TRUE))
    ) # end POST
  }
  # parse jason and extract data
  rd <- parse_json(response)
  rd <- rd$data
  if (length(rd) == 0) {
    warning('no data found')
    return(NULL)
  }
  # convert json list into data.frame by looping through each holding
  df <- data.frame(
    Name = NA,
    Cusip = NA,
    Ticker = NA,
    Identifier = NA,
    Units = NA,
    Value = NA,
    TimeStamp = NA
  )
  robcheck <- function(x, l) {
    if (x %in% names(l)) {
      if (!is.null(l[[x]])) {
        return(l[x])
      } else {
        return(NA)
      }
    } else {
      return(NA)
    }
  }
  for (i in 1:length(rd)) {
    df[i, 'Name'] <- rd[[i]]$asset['name'][[1]]
    df[i, 'Cusip'] <- robcheck('cusip', rd[[i]]$asset)
    df[i, 'Ticker'] <- robcheck('ticker', rd[[i]]$asset)
    df[i, 'Identifier'] <- robcheck('identifier', rd[[i]]$asset)
    df[i, 'Units'] <- rd[[i]]$returnInfo['units'][[1]]
    df[i, 'Value'] <- rd[[i]]$returnInfo['emv'][[1]]
    df[i, 'TimeStamp'] <- rd[[i]]$returnInfo['returnDate'][[1]]
  }
  df$CapWgt <- df$Value / sum(df$Value, na.rm = TRUE)
  return(df)
}

#' @title Download latest N-Port data from SEC EDGAR
#' @param long_cik fund specific CIK
#' @param short_cik parent company CIK
#' @param user_email email address needs to be provided for scraping SEC data
#'   (no login or registration needed)
#' @return \code{data.frame} with holdings and meta-data
#' @examples
#' # GMO Quality (GQEFX)
#' download_sec("S000004084", "772129", "asotolongo@diversifiedtrust.com")
#' @export
download_sec <- function(long_cik, short_cik, user_email) {
  doc_type <- 'nport-p'
  url <- paste0(
    'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=',
    long_cik,
    '&type=',
    doc_type,
    '&dateb=&count=5&scd=filings&search_text=')
  # SEC requires email as a header for authentication, you don't have to
  # register email on their site, just need valid email address
  response <- httr::GET(url, add_headers(`User-Agent` = user_email))
  html_doc <- rvest::read_html(response)
  tbl <- rvest::html_table(html_doc)
  # might need to change which table to target if web design changes
  long_str <- tbl[[3]]$Description[1]
  file_date <- tbl[[3]]$`Filing Date`[1]
  # need to extract ##-##-## from table cell to target the url of the latest
  # filing
  num_id <- stringr::str_extract(long_str,
                                 '[[:digit:]]+-[[:digit:]]+-[[:digit:]]+')
  num_str <- gsub('-', '', num_id)
  # now we can target url of latest filing and get the holdings
  url <- paste0('https://www.sec.gov/Archives/edgar/data/',
                short_cik, '/', num_str, '/primary_doc.xml')
  response <- httr::GET(url, add_headers(`User-Agent` = user_email))
  xml_file <- xml2::read_xml(response)
  doc <- XML::xmlParse(xml_file)
  root <- XML::xmlRoot(doc)
  # xmlToList can take ~10+ seconds for large amount of holdings, is there more efficient way?
  all_list <- XML::xmlToList(root)
  # current node is invstOrSecs for the holdings
  all_sec <- all_list$formData$invstOrSecs
  xdf <- data.frame(
    Name = NA,
    Lei = NA,
    Cusip = NA,
    Isin = NA, # conditional, required to list at least one additional ID
    Identifier = NA, # ID is usually ISIN, check for ISIN otherwise call alt
    Value = NA,
    Units = NA,
    Currency = NA,
    CapWgt = NA,
    Payoff = NA,
    AssetCat = NA,
    IssuerCat = NA,
    Country = NA,
    TimeStamp = NA,
    ValueLevel = NA,
    Maturity = NA,
    CouponType = NA,
    CouponRate = NA
  )
  for (i in 1:length(all_sec)) {
    x <- all_sec[[i]]
    xdf[i, 'Name'] <- get_list_fld(x, "name")
    xdf[i, 'Lei'] <- get_list_fld(x, "lei")
    xdf[i, 'Cusip']  <- get_list_fld(x, "cusip")
    if ('isin' %in% names(x$identifiers)) {
      xid <- x$identifiers
      xdf[i, 'Isin'] <- get_list_fld(xid, "isin")
      xdf[i, 'Identifier'] <- get_list_fld(xid, "altid")
    }
    xdf[i, 'Value'] <- get_list_fld(x, "valUSD")
    xdf[i, 'Units'] <- get_list_fld(x, "units")
    if ('curCd' %in% names(x)) {
      xdf[i, 'Currency'] <- get_list_fld(x, "curCd")
    }
    if ('currencyConditional' %in% names(x)) {
      xdf[i, 'Currency'] <- get_list_fld(x, "curCd")
    }
    xdf[i, 'CapWgt'] <- get_list_fld(x, "pctVal")
    xdf[i, 'Payoff'] <- get_list_fld(x, "payoffProfile")
    xdf[i, "AssetCat"] <- get_list_fld(x, "assetCat")
    xdf[i, "IssuerCat"] <- get_list_fld(x, "issuerCat")
    xdf[i, 'Country'] <- get_list_fld(x, "invCountry")
  }
  xdf[ ,'TimeStamp'] <- as.Date(file_date)
  xdf$CapWgt <- as.numeric(xdf$CapWgt)
  xdf$CapWgt <- xdf$CapWgt / 100
  return(xdf)
}

#' @title Factset Global Prices API for total returns
#' @param api_keys list of api keys
#' @param ids character vector of ids to download
#' @param date_start beginning date, earliest start is 2006-01-03
#' @param date_end ending date
#' @param freq "D", or "M" for daily or monthly time-series
#' @return json with data
#' @seealso \code{\link{flatten_fs_global_prices}} for converting json list into
#'   a data.table
#' @examples
#' load("~/api_keys.RData")
#' download_fs_global_prices(
#'   api_keys = api_keys,
#'   ids = c("AAPL", "MSFT"),
#'   date_start = "2025-05-01",
#'   date_end = "2025-05-15",
#'   freq = "D")
#' @export
download_fs_global_prices <- function(api_keys, ids, date_start, date_end,
                                      freq = "D") {
  username <- api_keys$fs$username
  password <- api_keys$fs$password
  ids[is.na(ids)] <- ""
  url <- "https://api.factset.com/content/factset-global-prices/v1/returns"
  request <- list(
    ids = as.list(ids),
    startDate = date_start,
    endDate = date_end,
    frequency = freq,
    currency = "USD",
    dividendAdjust = "EXDATE_C",
    batch = "N"
  )
  response <- httr::POST(
    url, authenticate(username, password), body = request,
    add_headers(Accept = 'application/json'), encode = 'json')
  output <- rawToChar(response$content)
  json <- parse_json(output)
  return(json)
}

#' @title Factset Global Prices API for End of Day Prices
#' @param api_keys list of api keys
#' @param ids character vector of ids to download
#' @param date_start begging date, earliest start is 2006-01-03
#' @param date_end ending date
#' @param freq "D", or "M" for daily or monthly time-series
#' @return json with data
#' @seealso \code{\link{flatten_fs_global_prices}} for converting json list into
#'   a data.table
#' @examples
#' load("~/api_keys.RData")
#' download_fs_exchange_price(
#'   api_keys = api_keys,
#'   ids = c("AAPL", "MSFT"),
#'   date_start = "2025-05-01",
#'   date_end = "2025-05-15",
#'   freq = "D")
#' @export
download_fs_exchange_price <- function(api_keys, ids, date_start, date_end,
                                       freq = "D") {
  username <- api_keys$fs$username
  password <- api_keys$fs$password
  ids[is.na(ids)] <- ""
  url <- "https://api.factset.com/content/factset-global-prices/v1/prices"
  request <- list(
    ids = as.list(ids),
    startDate = date_start,
    endDate = date_end,
    frequency = freq,
    fields = list("price"),
    batch = "N"
  )
  response <- httr::POST(
    url, authenticate(username, password), body = request,
    add_headers(Accept = 'application/json'), encode = 'json')
  output <- rawToChar(response$content)
  json <- parse_json(output)
  return(json)
}

#' @title Download Last Business Day Price
#' @param api_keys list of api keys
#' @param ids character vector of ids to download
#' @param as_of leave NULL for last trading day or specify as_of date for price
#' @return data.frame with prices
#' @examples
#' load("~/api_keys.RData")
#' download_latest_price(api_keys, c("AAPL", "MSFT"), as.Date("2025-05-01"))
#' @export
download_latest_price <- function(api_keys, ids, as_of = NULL) {
  as_of <- last_us_trading_day(as_of)
  json <- download_fs_exchange_price(api_keys, ids, as_of, as_of)
  flatten_fs_global_prices(json, TRUE)
}

#' @title Factset Formula API: RA_RET
#' @param id character representing id (only supports one id)
#' @param t_minus number of months of desired time-series, numeric value
#' @param freq "D", or "M" for daily or monthly time-series
#' @return xts time-series
#' @note
#'   For downloading returns of investments not on an exchange, e.g., mutual
#'   funds, indexes, DTC models, etc
#' @examples
#' load("~/api_keys.RData")
#' download_fs_ra_ret(
#'   id = "GQETX",
#'   api_keys = api_keys,
#'   t_minus = 1,
#'   freq = "D"
#' )
#' @export
download_fs_ra_ret <- function(id, api_keys, t_minus = 12, freq = 'D') {
  username <- api_keys$fs$username
  password <- api_keys$fs$password
  base_url <- "https://api.factset.com/formula-api/v1/time-series?ids=$IDS&formulas="
  request <- paste0(
    base_url,
    'RA_RET(',
    '"',
    id,
    '"',
    ",",
    "-",
    t_minus,
    "/0/0,0,", freq,
    ",FIVEDAY,USD,1)&flatten=Y"
  )
  response <- httr::GET(request, authenticate(username, password))
  output <- rawToChar(response$content)
  dat <- parse_json(output)
  dat <- dat[[1]]
  dt <- lapply(dat, '[[', 'date')
  dt <- list_replace_null(dt)
  val <- lapply(dat, '[[', 2)
  val <- list_replace_null(val)
  res <- xts(unlist(val) / 100, as.Date(unlist(dt)))
  colnames(res) <- id
  return(res)
}

#' @title Download Factset formula API
#' @param api_keys list containing api keys
#' @param ids vector of ids to download
#' @param formulas formula to download
#' @param type "ts" for times-series or "cs" for cross-sectional
#' @param flatn boolean to add "flatten=Y" to the api URL
#' @return json data
#' @seealso \code{\link{flatten_fs_formula}} to transform json list into a
#'   data.frame
#' @examples
#' load("~/api_keys.RData")
#' download_fs_formula(
#'   api_keys = api_keys,
#'   ids = c("AAPL", "MSFT"),
#'   formulas = "FG_PE(-1AY,NOW,CQ)")
#' @export
download_fs_formula <- function(api_keys, ids, formulas, type = c('ts', 'cs'),
                                flatn = TRUE) {
  if (type[1] == 'ts') {
    struc <- 'time-series'
  } else {
    struc <- 'cross-sectional'
  }
  username <- api_keys$fs$username
  password <- api_keys$fs$password
  ids[is.na(ids)] <- ""
  request <- paste0(
    "https://api.factset.com/formula-api/v1/",
    struc,
    "?ids=",
    paste0(ids, collapse = ","),
    "&formulas=",
    paste0(formulas, collapse = ",")
  )
  if (flatn) {
    request <- paste0(request, '&flatten=Y')
  }
  response <- httr::GET(request, authenticate(username, password))
  print(response$status)
  output <- rawToChar(response$content)
  json <- parse_json(output)
  return(json)
}

#' @title Utility to flatten json download
#' @param json download from \code{\link{download_fs_formula}}
#' @return data.frame
#' @export
flatten_fs_formula <- function(json) {
  create_df <- function(x) {
    y <- try(as.data.frame(x))
    if ("try-error" %in% class(y)) {
      return(data.frame())
    } else {
      return(y)
    }
  }
  df_list <- lapply(json$data, create_df)
  do.call("rbind", df_list)
}

#' @title Utility to flatten json download
#' @param json download from \code{\link{download_fs_global_prices}}
#' @param price boolean, "TRUE" if price data, "FALSE" if return data
#' @return data.frame
#' @export
flatten_fs_global_prices <- function(json, price = FALSE) {
  if ('status' %in% names(json)) {
    if (json$status == "Bad Request") {
      warning('bad request, returning empty data.frame')
      return(data.frame())
    }
  }
  dat <- json$data
  requestId <- sapply(dat, '[[', 'requestId')
  if (is.list(requestId)) {
    requestId <- unlist(list_replace_null(requestId))
  }
  date <- sapply(dat, '[[', 'date')
  if (is.list(date)) {
    date <- unlist(list_replace_null(date))
  }
  if (price) {
    price <- sapply(dat, '[[', "price")
    if (is.list(price)) {
      price <- unlist(list_replace_null(price))
    }
    df <- data.frame(
      RequestId = requestId,
      Date = date,
      Price = price
    )
  } else {
    totalReturn <- sapply(dat, '[[', 'totalReturn')
    if (is.list(totalReturn)) {
      totalReturn <- unlist(list_replace_null(totalReturn))
    }
    df <- data.frame(
      RequestId = requestId,
      Date = date,
      TotalReturn = totalReturn
    )
  }
  return(df)
}

#' @title Read Private I Excel Export
#' @param file_nm full file name of workbook
#' @param field_nm column name for returns
#' @return xts object of returns
#' @examples
#' read_private_xts(
#'   file_nm = "N:/Investment Team/DATABASES/CustomRet/PE-Downloads/PrivateEquity.xlsx",
#'   field_nm = "Private Equity"
#' )
#' @export
read_private_xts <- function(file_nm, field_nm) {
  dat <- readxl::read_excel(file_nm)
  dat_xts <- xts(dat$`Return*`, as.Date(dat$Date))
  colnames(dat_xts) <- field_nm
  return(dat_xts)
}

#' @title Read HFR Returns CSV Export
#' @param file_nm file name of csv file, e.g.,
#'   "C:/users/asotolongo/Downloads/HFR Indices.csv"
#' @return xts object of returns
#' @examples
#' dat <- read_hfr_csv("C:/users/asotolongo/Downloads/HFR Indices.csv")
#' tail(dat[, 1:5])
#' @export
read_hfr_csv <- function(file_nm) {
  dat <- read.csv(file_nm)
  dt <- colnames(dat)[8:ncol(dat)]
  dt <- gsub("X", "", dt)
  dt <- paste0(dt, ".01")
  dt <- as.Date(dt, format = "%Y.%m.%d")
  dt <- eo_month(dt)
  r <- xts(t(dat[, 8:ncol(dat)]), dt)
  colnames(r) <- dat$FUND_NAME
  return(r)
}

#' @title Upload Month End CTF Reconciled Returns into the Database
#' @param ac arcticDB datastore
#' @param xl_path filepath of excel workbook with returns, leave `NULL` for
#'   default
#' @param skip leading header rows to skip, default is `4`
#' @return no data is returned, the entire history is read and stored in
#'   arcticDB
#' @export
upload_ctf_monthly <- function(ac, xl_path = NULL, skip = 4) {
  lib <- get_all_lib(ac)
  tbl_cust <- lib$`meta-tables`$read("cust")$data
  if (is.null(xl_path)) {
    xl_path <- "N:/Investment Team/DATABASES/FACTSET/BMO NAV & Platform Return Upload.xlsx"
  }
  dat <- read_xts(xl_path, skip = skip)
  ix <- match(tbl_cust$WorkupId, colnames(dat))
  if (any(is.na(ix))) {
    if (all(is.na(ix))) {
      stop("no values found")
    }
    warning("missing values created when matching workup excel to custodian library")
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
  x <- dat[, tgt[1:3]]
  colnames(x)<- c(
    "Private Diversifiers",
    "Private Diversifiers II",
    "Short Duration"
  )
  r <- xts_cbind(r, x)
  r <- r["1994/"] / 100
  old_ret <- lib$returns$read("ctf")$data
  combo <- xts_rbind(xts_to_arc(r), old_ret,is_xts = FALSE, backfill = TRUE)
  lib$returns$write("ctf", xts_to_arc(combo))
}

read_daily_ctf_xl <- function(nm) {
  fnm <- paste0("N:/Investment Team/DATABASES/CustomRet/ctf-daily-backfill/",
                nm, ".csv")
  dat <- read.csv(fnm)
  dt <- try(as.Date(dat$Date))
  if (inherits(dt, "try-error")) {
    dt <- as.Date(dat$Date, format = "%m/%d/%Y")
  }
  tgt <-  c("BMV", "EMV", "Net.Additions")
  dat[, tgt] <- apply(dat[, tgt], 2, gsub, pattern = ",", replacement = "")
  dat[, tgt] <- apply(dat[ ,tgt], 2, as.numeric)
  r <- dat[, "EMV"] / (dat[, "BMV"] + dat[, "Net.Additions"]) - 1
  out <- xts(r, dt)
  colnames(out) <- nm
  return(out)
}

pvt_cf <- function(wb, cf_sht = "cf", nav_sht = "nav") {
  cf <- read_xts(wb, cf_sht)
  nav <- read_xts(wb, nav_sht)
  inter <- intersect(colnames(cf), colnames(nav))
  nav <- nav[, inter]
  cf <- cf[, inter]
  res <- list()
  for (i in 1:ncol(nav)) {
    ix <- paste0(zoo::index(na.omit(cf[, i]))[1], "/")
    combo <- clean_ret(cbind(cf[ix, i], nav[ix, i]), trunc_start = FALSE, eps = 1)
    q <- combo$ret[, 2]
    q <- q[q!= 0, ]
    twr <- q
    twr[,] <- NA
    cfi <- combo$ret[, 1]
    #cfj <- cut_time(cfi, date_end = zoo::index(q)[1])
    cfj <- cfi[paste0("/", zoo::index(q)[1])]
    w <- wgt_month(zoo::index(q)[1], zoo::index(cfj))
    twr[1] <- (as.numeric(q[1]) + sum(cfj)) / -sum(cfj * w)
    for (j in 2:nrow(q)) {
      #cfj <- cut_time(cfi, date_start = zoo::index(q)[j-1]+1, zoo::index(q)[j])
      date_start <- zoo::index(q)[j-1]+1
      date_end <- zoo::index(q)[j]
      cfj <- cfi[paste0(date_start, "/", date_end)]
      v1 <- as.numeric(q[j])
      v0 <- as.numeric(q[j-1])
      w <- wgt_month(zoo::index(q)[j], zoo::index(cfj))
      twr[j] <- (v1 - v0 + sum(cfj)) / (v0 - sum(cfj * w))
    }
    res[[i]] <- twr
  }
}

#' @export
wgt_month <- function(q, m) {
  x <- month(q) - month(m)
  wgt <- rep(NA, length(x))
  for (i in 1:length(x)) {
    if (x[i] == 0) {
      wgt[i] <- c(90 - 75) / 90
    }
    if (x[i] == 1) {
      wgt[i] <- (90 - 45) / 90
    }
    if (x[i] == 2) {
      wgt[i] <- (90 - 15) / 90
    }
  }
  return(wgt)
}
