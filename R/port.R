#' @title Remove Duplicate Dates in Holdings File
#' @description
#'  Problems can arise if duplicate multiple holdings are written
#'   for the same date, e.g., if was the holdings were accidently gathered
#'   and rowbinded to the holdings file twice for the same date. This function
#'   is intended to search for those occurances by finding duplications of a
#'   date and ID and removing them.
#' @param tbl_hold data.frame of holdings
#' @param id ID column name to use for duplicates, e.g., Name, Ticker, Cusip
#' @return tbl_hold with any duplicate dates removed
#' @export
remove_dup_dates <- function(tbl_hold, id = "Name") {
  is_dup <- duplicated(paste0(tbl_hold$TimeStamp, tbl_hold[[id]]))
  tbl_hold <- tbl_hold[!is_dup, ]
  return(tbl_hold)
}

#' @title Check if MSL has been merged with holdings file
#' @param x data.frame with holdings data
#' @return TRUE if MSL has been merged, FALSE if not
#' @export
check_msl_fields <- function(x) {
  all(c("DtcName", "Ticker", "Layer", "Cusip", "Sedol", "Isin", "Lei",
        "Identifier", "SecType", "ReturnLibrary") %in% colnames(x))
}

#' @title Subset the holdings file for the most recent date
#' @param tbl_hold data.frame of holdings
#' @return tbl_hold with most recent holdings
#' @export
latest_holdings <- function(tbl_hold) {
  if (!"TimeStamp" %in% colnames(tbl_hold)) {
    warning("no TimeStamp found")
    return(tbl_hold)
  }
  if (all(is.na(tbl_hold$TimeStamp))) {
    warning("all TimeStamp observations are missing")
    return(tbl_hold)
  }
  tbl_hold$TimeStamp <- force_date(tbl_hold$TimeStamp)
  is_latest <- tbl_hold$TimeStamp == max(tbl_hold$TimeStamp)
  tbl_hold[is_latest, ]
  tbl_hold[is_latest, ]
}

#' @title Drilldown to a lower layer
#' @param tbl_hold data.frame with holdings
#' @param layer numeric value for which layer to drilldown to
#' @param latest boolean to subset for most recent holdings
#' @return list with underlying holdings data.frame and a data.frame
#'   for holdings not found in the MSL
#' @export
# drill_down <- function(tbl_hold, layer = 1, latest = TRUE) {
#   if (latest) {
#     tbl_hold <- latest_holdings(tbl_hold)
#   }
#   tbl_miss <- data.frame()
#   tbl_msl <- read_msl(bucket)
#   if (!check_msl_fields(tbl_hold)) {
#     res <- merge_msl(tbl_hold, tbl_msl)
#     tbl_miss <- res$miss
#     tbl_hold <- res$inter
#   } else {
#     tbl_miss <- tbl_hold[is.na(tbl_hold$DtcName), ]
#   }
#   is_lay_1 <- tbl_hold$Layer <= layer
#   if (all(is_lay_1)) {
#     warning("no layers beyond 1 found")
#     return(tbl_hold)
#   }
#   lay_1 <- tbl_hold[is_lay_1, ]
#   x <- tbl_hold[!is_lay_1, ]
#   for (i in 1:10) {
#     for (j in 1:nrow(x)) {
#       record <- read_hold(x$DtcName[j], bucket)[[1]]
#       if (latest) {
#         record <- latest_holdings(record)
#       }
#       record[, paste0("Layer", x$Layer[j])] <- x$DtcName[j]
#       record$CapWgt <- record$CapWgt * x$CapWgt[j]
#       lay_1 <- rob_rbind(lay_1, record)
#     }
#     res <- merge_msl(lay_1, tbl_msl, FALSE)
#     lay_1 <- res$inter
#     tbl_miss <- rob_rbind(tbl_miss, res$miss)
#     is_lay_1 <- lay_1$Layer <= layer + i - 1
#     if (any(is.na(is_lay_1))) {
#       warning("some layer observations missing in tbl_msl")
#       is_lay_1[is.na(is_lay_1)] <- TRUE
#     }
#     x <- lay_1[!is_lay_1, ] # case where lay 2 invests in lay 2
#     if (nrow(x) == 0) {
#       break
#     }
#   }
#   lay_1 <- lay_1[lay_1$Layer == 1, ]
#   tbl_hold <- lay_1
#   res <- list()
#   res$tbl_hold <- tbl_hold
#   res$tbl_miss <- tbl_miss
#   return(res)
# }

drill_down <- function(bucket, tbl_hold) {
  tbl_msl <- read_msl(bucket)
  tbl_hold <- latest_holdings(tbl_hold)
  top_layer_res <- merge_msl(tbl_hold, tbl_msl)
  tbl_hold <- top_layer_res$inter
  res <- list()
  for (i in 1:10) {
    tbl_hold$Layer[is.na(tbl_hold$Layer)] <- 1
    if (all(tbl_hold$Layer == 1)) {
      break
    }
    ix <- tbl_hold$Layer > 1
    u_hold <- read_hold(tbl_hold$DtcName[ix], bucket)
    u_hold <- lapply(u_hold, latest_holdings)
    for (j in 1:length(u_hold)) {
      u_hold[[j]][[paste0("Parent", i)]] <- names(u_hold)[j]
    }
    nm_ix <- match(names(u_hold), tbl_hold$DtcName)
    wgt_vec <- tbl_hold$CapWgt[nm_ix]
    u_hold_adj <- mapply(
      \(x, w) {
        x$CapWgt <- x$CapWgt * w
        return(x)},
      x = u_hold, w = wgt_vec, SIMPLIFY = FALSE)
    u_hold_adj <- bind_rows_with_na(u_hold_adj)
    tbl_hold <- bind_rows_with_na(list(tbl_hold[!ix, ], u_hold_adj))
    res[[i]] <- merge_msl(tbl_hold, tbl_msl, FALSE)
    tbl_hold <- res[[i]]$inter
  }
}

bind_rows_with_na <- function(df_list) {
  # Extract all unique column names across all dataframes
  all_cols <- unique(unlist(lapply(df_list, names)))

  # If no columns exist in any dataframe, return empty data.frame
  if (length(all_cols) == 0) {
    return(data.frame())
  }

  # Standardize each dataframe to have all columns, filling missing ones with NA
  standardized_list <- lapply(df_list, function(df) {
    # If df has zero columns, create empty df with correct column names
    if (nrow(df) == 0) {
      df <- as.data.frame(matrix(ncol = length(all_cols), nrow = 0))
      names(df) <- all_cols
      return(df)
    }

    # Add missing columns with NA
    missing_cols <- setdiff(all_cols, names(df))
    for (col in missing_cols) {
      df[[col]] <- NA
    }

    # Reorder columns to match the full set
    df <- df[all_cols]
    return(df)
  })

  # Row bind all standardized dataframes
  result <- do.call(rbind, standardized_list)
  return(result)
}


