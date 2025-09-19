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

#' @export
drill_down <- function(bucket, tbl_hold) {
  tbl_msl <- read_msl(bucket)
  tbl_hold <- latest_holdings(tbl_hold)
  top_layer_res <- merge_msl(tbl_hold, tbl_msl)
  res <- list()
  res[[1]] <- list(top_layer_res)
  names(res[[1]]) <- "Top"
  for (i in 1:10) {
    layer <- sapply(res[[i]], \(x) {x$inter$Layer}) |>
      as.vector()
    if (inherits(layer, "list")) {
      layer <- unlist(layer)
    }
    layer[is.na(layer)] <- 1
    if (all(layer == 1)) {
      break
    }
    u_hold <- list()
    for (j in 1:length(res[[i]])) {
      ix <- res[[i]][[j]]$inter$Layer > 1
      ix[is.na(ix)] <- FALSE
      if (sum(ix) == 0) {
        next
      }
      u_hold_j <- read_hold(res[[i]][[j]]$inter$DtcName[ix], bucket)
      u_hold_j <- lapply(u_hold_j, latest_holdings)
      u_hold_j <- lapply(u_hold_j, merge_msl, tbl_msl = tbl_msl,
                         rm_dup_dates = FALSE)
      u_hold <- c(u_hold, u_hold_j)
    }
    res[[i+1]] <- u_hold
  }
  return(res)
}



#' @export
load_qual <- function(bucket) {
  fina <- try_read(bucket, "co-data/fina-latest.parquet")
  sect <- try_read(bucket, "co-data/sector.parquet")
  country <- try_read(bucket, "co-data/country.parquet")
  macro_r3 <- try_read(bucket, "co-data/macro_sel_r3.parquet")
  return(list(fina, sect, country, macro_r3))
}

#' @export
merge_qual <- function(tbl_hold, list_data) {
  for (i in 1:length(list_data)) {
    tbl_hold <- left_merge_flat(tbl_hold, list_data[[i]], "DtcName")
  }
  return(tbl_hold)
}

#' @export
left_merge_flat <- function(x, y, match_by, keep_x_dup_col = FALSE)  {
  ix <- match_mult(x, y, match_by)
  dup_col <- colnames(y) %in% colnames(x)
  if (keep_x_dup_col) {
    cbind(x, y[ix, ], drop = FALSE)
  } else {
    cbind(x, y[ix, !dup_col, drop = FALSE])
  }
}

#' @export
qual_roll_up <- function(res, bucket, qual = NULL) {

  # columns to total
  qual_cols <- c("PE", "PB", "PFCF", "DY", "MacroSelect1", "MacroSelect2",
                 "MacroSelect3", "MacroSelect4", "MacroSelectRank")
  if (is.null(qual)) {
    qual <- load_qual(bucket)
  }
  n <- length(res)
  # if just one layer, then all data are level 1, total up and return
  if (n == 1) {
    res[[1]][[1]]$inter <- merge_qual(res[[1]][[1]]$inter, qual)
    total[[1]] <- total_qual(res[[1]][[1]]$inter)
    out <- list()
    out$res <- res
    out$total <- total
    return(out)
  }
  # else we need to keep track of multiple layers
  total <- list()
  # merge qualitative data and take first pass at totaling
  for (i in n:1) {
    for (j in 1:length(res[[i]])) {
      res[[i]][[j]]$inter <- merge_qual(res[[i]][[j]]$inter, qual)
    }
    total[[i]] <- lapply(res[[i]], \(x){total_qual(x$inter)})
  }
  # loop backwards to total up lower levels first, e.g., total up stocks,
  # then managers, then CTF
  for (i in n:2) {
    for (j in 1:length(res[[i-1]])) {
      x <- res[[i-1]][[j]]
      ix <- match(names(total[[i]]), x$inter$DtcName)
      if (any(!is.na(ix))) {
        ix_df <- data.frame(Total = 1:length(ix), X = ix)
        ix_df <- na.omit(ix_df)
        for (k in 1:nrow(ix_df)) {
          total_df <- as.data.frame(total[[i]][[ix_df$Total[k]]])
          try(x$inter[ix_df$X[k], qual_cols] <- total_df[, qual_cols])
        }
      }
      res[[i-1]][[j]] <- x
    }
    total[[i]] <- lapply(res[[i]], \(x){total_qual(x$inter)})
  }
  # finally total up the top layer
  ix <- match(names(total[[2]]), res[[1]][[1]]$inter$DtcName)
  if (any(!is.na(ix))) {
    ix_df <- data.frame(Total = 1:length(ix), X = ix)
    ix_df <- na.omit(ix_df)
    for (i in 1:length(ix)) {
      total_df <- as.data.frame(total[[2]][[ix_df$Total[i]]])
      try(res[[1]][[1]]$inter[ix_df$X[i], qual_cols] <- total_df[, qual_cols])
    }
  }
  total[[1]][[1]] <- total_qual(res[[1]]$Top$inter)
  # return data, res and total
  out <- list()
  out$res <- res
  out$total <- total
  return(out)
}

flatten_drill_down <- function(res) {
  n <- length(res$res)
  flat <- res$res
  for (i in n:2) {
    for (j in 1:length(res$res[[i-1]])) {
      x <- flat[[i-1]][[j]]
      x$inter[[paste0("Parent", i)]] <- x$inter$DtcName
      ix <- match(names(res$res[[i]]), x$inter$DtcName)
      if (any(!is.na(ix))) {
        ix_df <- data.frame(A = 1:length(ix), B = ix)
        ix_df <- na.omit(ix_df)
        insert_list <- list()
        for (k in 1:nrow(ix_df)) {
          w <- x$inter[ix_df$B[k], "CapWgt"]
          insert_df <- res$res[[i]][[ix_df$A[k]]]$inter
          insert_df[[paste0("Parent", i)]] <- names(res$res[[i]])[ix_df$A[k]]
          insert_df$CapWgt <- insert_df$CapWgt * w
          insert_list[[k]] <- insert_df
        }
        insert_list[[length(insert_list) + 1]] <- x$inter[-ix_df$B, ]
        x$inter <- bind_rows_with_na(insert_list)
      }
      flat[[i-1]][[j]] <- x
    }
  }
  res$flat <- flat
  return(res)
}


qual_group_by <- function(res, bucket, grp) {
  n <- length(res$res)
  if (length(n) == 1) {

  }
  group <- list()
  for (i in n:1) {
    group[[i]] <- lapply(res$res[[i]], \(x, grp) {group_tbl(x$inter, grp)}, grp)
  }
}

group_tbl <- function(tbl_hold, grp, parent = NULL, summ = "CapWgt") {
  if (grp == "No Group") {
    return(tbl_hold)
  }
  if (grp %in% c("FactsetSector", "GicsMacro", "GicsMap", "RiskCountry")) {
    if (!is.null(parent)) {
      if (parent %in% colnames(tbl_hold)) {
        p <- split(tbl_hold, tbl_hold[[parent]])
        g <- lapply(p, \(x) {split(x, x[[grp]])})
        l <- list()
        x <- lapply(g[[1]], total_qual)
        dat <- data.frame(Group = names(x), do.call(rbind, x),
                          row.names = NULL)
        dat <- dat[, c("Group", summ)]
        colnames(dat)[2] <- names(g)[1]
        for (i in 2:length(g)) {
          x <- lapply(g[[i]], total_qual)
          x <- data.frame(Group = names(x), do.call(rbind, x),
                               row.names = NULL)
          x <- x[, c("Group", summ)]
          colnames(x)[2] <- names(g)[i]
          dat <- left_merge_flat(dat, x, match_by = "Group")
        }
      }
    } else {
      g <- split(tbl_hold, tbl_hold[[grp]])
      x <- lapply(g, total_qual)
      dat <- data.frame(Group = names(x), do.call(rbind, x), row.names = NULL)
    }
  }
  if (grp %in% c("PE", "PB", "PFCF", "DY")) {

  }
  if (grp == "MacroSelectRank") {

  }
  return(dat)
}


#' @export
total_qual <- function(tbl_hold) {
  if (!"CapWgt" %in% colnames(tbl_hold)) {
    warning("tbl_hold is miss-specified")
    return(tbl_hold)
  }
  total <- list()
  total$CapWgt <- sum(tbl_hold$CapWgt, na.rm = TRUE)
  if ("Value" %in% colnames(tbl_hold)) {
    total$Value <- sum(as.numeric(tbl_hold$Value), na.rm = TRUE)
  } else {
    total$Value <- NA
  }
  if ("PE" %in% colnames(tbl_hold)) {
    total$PE <- wgt_harmonic_mean(tbl_hold$CapWgt, tbl_hold$PE)
  } else {
    total$PE <- NA
  }
  if ("PB" %in% colnames(tbl_hold)) {
    total$PB <- wgt_harmonic_mean(tbl_hold$CapWgt, tbl_hold$PB)
  } else {
    total$PB <- NA
  }
  if ("PFCF" %in% colnames(tbl_hold)) {
    total$PFCF <- wgt_harmonic_mean(tbl_hold$CapWgt, tbl_hold$PFCF)
  } else {
    total$PFCF <- NA
  }
  if ("DY" %in% colnames(tbl_hold)) {
    total$DY <- wgt_avg(tbl_hold$CapWgt, tbl_hold$DY)
  } else {
    total$DY <- NA
  }
  if ("MacroSelect1" %in% colnames(tbl_hold)) {
    total$MacroSelect1 <- wgt_avg(tbl_hold$CapWgt, tbl_hold$MacroSelect1)
  } else {
    total$MacroSelect1 <- NA
  }
  if ("MacroSelect2" %in% colnames(tbl_hold)) {
    total$MacroSelect2 <- wgt_avg(tbl_hold$CapWgt, tbl_hold$MacroSelect2)
  } else {
    total$MacroSelect2 <- NA
  }
  if ("MacroSelect3" %in% colnames(tbl_hold)) {
    total$MacroSelect3 <- wgt_avg(tbl_hold$CapWgt, tbl_hold$MacroSelect3)
  } else {
    total$MacroSelect3 <- NA
  }
  if ("MacroSelect4" %in% colnames(tbl_hold)) {
    total$MacroSelect4 <- wgt_avg(tbl_hold$CapWgt, tbl_hold$MacroSelect4)
  } else {
    total$MacroSelect4 <- NA
  }
  if ("MacroSelectRank" %in% colnames(tbl_hold)) {
    total$MacroSelectRank <- wgt_avg(tbl_hold$CapWgt, tbl_hold$MacroSelectRank)
  } else {
    total$MacroSelectRank <- NA
  }
  return(total)
}
