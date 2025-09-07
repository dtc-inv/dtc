
drill_down <- function(tbl_hold, tbl_msl, layer = 1, latest_date = FALSE) {
  if (!check_msl_fields(tbl_hold)) {
    res <- merge_msl(tbl_hold, tbl_msl)
    miss_msl_top <- res$miss
  }



}

remove_dup_dates <- function(tbl_hold, id = "Name") {
  is_dup <- duplicated(paste0(tbl_hold$TimeStamp, tbl_hold[[id]]))
  tbl_hold <- tbl_hold[!is_dup, ]
  return(tbl_hold)
}

check_msl_fields <- function(x) {
  all(c("DtcName", "Ticker", "Layer", "Cusip", "Sedol", "Isin", "Lei",
        "Identifier", "SecType", "ReturnLibrary") %in% colnames(x))
}

latest_holdings <- function(tbl_hold) {
  is_char <- "character" %in% class(tbl_hold$Timestamp)
  if (is_char) {
    tbl_hold$TimeStamp <- as.Date(tbl_hold$TimeStamp)
  }
  is_latest <- tbl_hold$TimeStamp == max(tbl_hold$TimeStamp)
  if (is_char) {
    tbl_hold$TimeStamp <- as.character(tbl_hold$TimeStamp)
  }
  tbl_hold[is_latest, ]
}
