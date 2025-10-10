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

#' @title Drilldown into holdings table
#' @param bucket S3 file system
#' @param tbl_hold data.frame of portfolio holdings
#' @return list with different layers
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
  macro <- try_read(bucket, "co-data/macro_sel_metrics.parquet")
  return(list(fina, sect, country, macro_r3, macro))
}

#' @title Merge Qualitative Data with holdings
#' @param tbl_hold data.frame with portfolio holdings
#' @param list_data list of data.frames with Qualitative Data
#' @note tables are merged based on DtcName field, left join
#' @export
merge_qual <- function(tbl_hold, list_data) {
  for (i in 1:length(list_data)) {
    tbl_hold <- left_merge_flat(tbl_hold, list_data[[i]], "DtcName")
  }
  return(tbl_hold)
}

#' @title Utility function to return left merge of x and y as data.frame
#' @param x data.frame
#' @param y data.frame to form left join
#' @param match_by string to represent which field (column) to match
#' @param keep_x_dup_col boolean to keep duplicate columns in the x data.frame,
#'   default is FALSE meaning common fields from y will replace counterparts in
#'   x
#' @return data.frame of x, y join
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

#' @title Roll up qualitative data
#' @param res output from drill_down() function
#' @param bucket s3 file system
#' @param qual optional list of qualitative data, leave NULL to load defaults
#' @return list with qualitative data merged and total slot for totals
#' @export
qual_roll_up <- function(res, bucket, qual = NULL) {

  # columns to total
  qual_cols <- c("PE", "PB", "PFCF", "DY", "MacroSelect1", "MacroSelect2",
                 "MacroSelect3", "MacroSelect4", "MacroSelectRank",
                 "F1", "F2", "F3", "F4")
  if (is.null(qual)) {
    qual <- load_qual(bucket)
  }
  n <- length(res)
  total <- list()
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

#' @title Flatten drill down list
#' @param res output from qual_roll_up() function
#' @return adds flat slot with data.frames
#' @export
flatten_drill_down <- function(res) {
  n <- length(res$res)
  flat <- res$res
  if(n == 1) {
    res$flat <- flat
    return(res)
  }
  for (i in n:2) {
    for (j in 1:length(res$res[[i-1]])) {
      x <- flat[[i-1]][[j]]
      x$inter[[paste0("Parent", i-1)]] <- names(flat[[i-1]])[j]
      ix <- match(names(res$res[[i]]), x$inter$DtcName)
      if (any(!is.na(ix))) {
        ix_df <- data.frame(A = 1:length(ix), B = ix)
        ix_df <- na.omit(ix_df)
        insert_list <- list()
        for (k in 1:nrow(ix_df)) {
          w <- x$inter[ix_df$B[k], "CapWgt"]
          insert_df <- flat[[i]][[ix_df$A[k]]]$inter
          insert_df[[paste0("Parent", i-1)]] <- names(flat[[i-1]])[j]
          insert_df[[paste0("Parent", i)]] <- names(res$res[[i]])[ix_df$A[k]]
          insert_df$CapWgt <- insert_df$CapWgt * w
          insert_list[[k]] <- insert_df
        }
        insert_list[[length(insert_list) + 1]] <- x$inter[-ix_df$B, ]
        x$inter <- bind_rows_with_na(insert_list)
        is_miss <- is.na(x$inter[[paste0("Parent", i)]])
        x$inter[is_miss, paste0("Parent", i)] <-
          paste0(x$inter[is_miss, paste0("Parent", i-1)], " Layer 1")
      }
      flat[[i-1]][[j]] <- x
    }
  }
  res$flat <- flat
  return(res)
}

#' @export
qual_group_by <- function(res, bucket, grp) {
  n <- length(res$res)
  if (length(n) == 1) {

  }
  group <- list()
  for (i in n:1) {
    group[[i]] <- lapply(res$res[[i]], \(x, grp) {group_tbl(x$inter, grp)}, grp)
  }
}

#' @export
group_tbl <- function(tbl_hold, grp, parent = NULL, summ = "CapWgt") {
  if (grp == "No Group") {
    return(tbl_hold)
  }
  if (grp %in% c("FactsetSector", "GicsMacro", "GicsMap", "RiskCountry",
                 "Region")) {
    if (!is.null(parent)) {
      if (parent %in% colnames(tbl_hold)) {
        p <- split(tbl_hold, tbl_hold[[parent]])
        g <- lapply(p, \(x) {split(x, x[[grp]])})
        l <- list()
        dat <- data.frame(Group = unique(tbl_hold[[grp]]))
        for (i in 1:length(g)) {
          x <- lapply(g[[i]], total_qual)
          if (length(x) == 0) {
            x <- data.frame(A = NA, B = NA)
            colnames(x) <- c("Group", names(g)[i])
          } else {
            x <- data.frame(Group = names(x), do.call(rbind, x),
                                 row.names = NULL)
            x <- x[, c("Group", summ)]
            colnames(x)[2] <- names(g)[i]
          }
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
  d <- lapply(dat[, -1], list_replace_null)
  x <- do.call(cbind, lapply(d, unlist))
  dat <- cbind(dat[, 1], as.data.frame(x, row.names = NULL))
  colnames(dat)[1] <- "Group"
  return(dat)
  return(dat)
}

#' @export
prep_hold <- function(bucket, tbl_hold) {
  res <- drill_down(bucket, tbl_hold)
  res <- qual_roll_up(res, bucket)
  flatten_drill_down(res)
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
  if ("F1" %in% colnames(tbl_hold)) {
    total$F1 <- wgt_avg(tbl_hold$CapWgt, tbl_hold$F1)
  }
  if ("F2" %in% colnames(tbl_hold)) {
    total$F2 <- wgt_avg(tbl_hold$CapWgt, tbl_hold$F2)
  }
  if ("F3" %in% colnames(tbl_hold)) {
    total$F3 <- wgt_avg(tbl_hold$CapWgt, tbl_hold$F3)
  }
  if ("F4" %in% colnames(tbl_hold)) {
    total$F4 <- wgt_avg(tbl_hold$CapWgt, tbl_hold$F4)
  }
  return(total)
}
