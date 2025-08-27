#' Rebal Object
#' 
#' @description
#' Object to handle rebalancing asset returns into a portfolio time-series of
#'   returns. Rebalance weights can either be a vector or xts time-series of
#'   weights. Helper function can add periodic (e.g., monthly) rebalanc weights
#'   based on the previous specified weights.
#' 
#' @export
Rebal <- R6::R6Class(
  "Rebal",
  public = list(
    #' @field name column name for returns after rebal
    name = NULL,
    #' @field rebal_wgt rebalance weights
    rebal_wgt = NULL,
    #' @field asset_wgt asset weights
    asset_wgt = NULL,
    #' @field asset_ret asset returns
    asset_ret = NULL,
    #' @field asset_freq frequency of asset returns
    asset_freq = NULL,
    #' @field rebal_freq frequency of rebalance
    rebal_freq = NULL,
    #' @field rebal_ret portfolio returns from 
    rebal_ret = NULL,
    #' @field ctr_mat contribution to return matrix
    ctr_mat = NULL,
    #' @field asset_ret_start inception of asset returns
    asset_ret_start = NULL,
    #' @field asset_ret_end last date of asset returns
    asset_ret_end = NULL,
    
    #' @description Create new rebal object
    #' @param rebal_wgt vector or xts of rebalance weights
    #' @param asset_ret xts of asset returns
    #' @param asset_freq character for asset_ret frequency, see below
    #' @param rebal_freq character: "D", "M", "Q", "A", "BH" for daily, monthly,
    #'   quarterly, annual, or buy and hold
    #' @param name column name for rebalance returns
    initialize = function(rebal_wgt, asset_ret, asset_freq = "D", 
                          rebal_freq = "M", name = "PortRebal") {
      self$rebal_wgt <- rebal_wgt
      self$asset_ret <- asset_ret
      self$rebal_freq <- rebal_freq
      self$asset_ret_start <- zoo::index(asset_ret)[1]
      self$asset_ret_end <- zoo::index(asset_ret)[nrow(asset_ret)]
      self$name <- name
    },
    
    #' @description change vector of weights into xts with start date of asset
    #'   returns
    wrangle_rebal_vect = function() {
      rebal_xts <- xts(matrix(self$rebal_wgt, nrow = 1), self$asset_ret_start)
      colnames(rebal_xts) <- names(self$rebal_wgt)
      self$rebal_wgt <- rebal_xts
    },
    
    #' @description match columns of rebal weights and asset returns
    align_rebal_wgt = function() {
      # if not colnames or colnames are default X.1 to X.N then check if number
      # of columns between asset_ret and rebal_wgt are equal
      if (is.null(colnames(self$rebal_wgt)) | 
          all(colnames(self$rebal_wgt) == paste0("X.", 1:ncol(self$rebal_wgt)))) {
        if (ncol(self$rebal_wgt) == ncol(self$asset_ret)) {
          warning("no names or colnames provided for rebal_wgt, assuming they
                  correspond to asset_ret")
          colnames(self$rebal_wgt) <- colnames(self$asset_ret)
        } else {
          stop("no names or colnames provided for rebal_wgt and number of columns
               does not match the number of asset_ret columns")
        }
      # if colnames are provided then find intersection
      } else {
        miss <- setdiff(colnames(self$rebal_wgt), colnames(self$asset_ret))
        if (identical(miss, colnames(self$rebal_wgt))) {
          stop("no intersection of rebal_wgt and asset_ret columns")
        }
        inter <- intersect(colnames(self$rebal_wgt), colnames(self$asset_ret))
        unn <- union(colnames(self$rebal_wgt), colnames(self$asset_ret))
        if (length(unn) > length(inter)) {
          warning(c(unn[!unn %in% inter], " not in intersection."))
        }
        self$rebal_wgt <- self$rebal_wgt[, inter]
        self$asset_ret <- self$asset_ret[, inter]
      }
    },
    
    #' @description fill in periodic rebalance dates if frequency is specified
    fill_rebal_dates = function() {
      # TO-DO: NEED FUNCTION TO CHECK REBAL AND RET FREQ
      # create date vector based on rebalance frequency, exit for BH
      start_date <- zoo::index(self$rebal_wgt)[1]
      if (self$rebal_freq == 'D') {
        dt_vec <- zoo::index(self$asset_ret)
      } else if (self$rebal_freq == 'M') {
        dt_vec <- seq(start_date, self$asset_ret_end, 'months')
        dt_vec <- lubridate::floor_date(dt_vec + 10, 'months') - 1
      } else if (self$rebal_freq == 'Q') {
        dt_vec <- seq(start_date, self$asset_ret_end, 'quarters')
        dt_vec <- lubridate::floor_date(dt_vec + 10, 'quarters') - 1
      } else if (self$rebal_freq == 'A') {
        dt_vec <- seq(start_date, self$asset_ret_end, 'years')
      } else if (self$rebal_freq == "BH") {
        return(invisible(self))
      } else {
        stop("rebal_wgt miss-specified")
      }
      # create a NA xts of dates, combine with rebalance dates, and fill missing
      # values based on last valid rebalance weight
      dt_mat <- matrix(as.numeric(NA), nrow = length(dt_vec), 
                       ncol = ncol(self$asset_ret))
      dt_xts <- xts(dt_mat, dt_vec)
      # remove any potential duplicate dates if existing rebalance on
      # period end
      colnames(dt_xts) <- colnames(self$rebal_wgt)
      is_dup <- zoo::index(dt_xts) %in% zoo::index(self$rebal_wgt)
      rebal_combo <- rbind(dt_xts[!is_dup, ], self$rebal_wgt)
      # first date can be missing, e.g., monthly rebalance created from vector 
      # rebal weights
      if (all(is.na(rebal_combo[1, ]))) {
        rebal_combo[1, ] <- self$rebal_wgt[1, ]
      }
      rebal_xts <- fill_na_price(rebal_combo)
      colnames(rebal_xts) <- colnames(self$Srebal_wgt)
      self$rebal_wgt <- rebal_xts
    },
    
    #' @description execute rebalance
    #' @param sum_to_1 boolean to force rebalance weights to equal 100% for each
    #'   period
    rebal = function(sum_to_1 = TRUE) {
      if (is.vector(self$rebal_wgt)) {
        self$wrangle_rebal_vect()
      }
      self$align_rebal_wgt()
      self$fill_rebal_dates()
      rebal_wgt <- self$rebal_wgt
      if (!all(rowSums(rebal_wgt) == 1)) {
        warning("row sums of rebal_wgt != 1, if sum_to_1 is set to FALSE will 
                cause return drag.")
      }
      if (sum_to_1) {
        rebal_wgt <- rebal_wgt / rowSums(rebal_wgt)
      }
      asset_ret <- self$asset_ret
      asset_ret <- cut_time(asset_ret, zoo::index(rebal_wgt)[1])
      rebal_dt <- zoo::index(rebal_wgt)
      ret_dt <- zoo::index(asset_ret)
      n_obs <- nrow(asset_ret)
      rebal_counter <- 1
      # asset index is the end of period wealth value
      asset_idx <- matrix(nrow = n_obs + 1, ncol = ncol(asset_ret))
      asset_idx[1, ] <- 100 * rebal_wgt[1, ]
      # asset weight is beginning of period weight
      asset_wgt <- matrix(nrow = n_obs, ncol = ncol(asset_ret))
      colnames(asset_wgt) <- colnames(asset_ret)
      # assumes rebalance happens at beginning of each day
      # (or month, year, etc),
      # and then that day's return is applied to create an end of day
      # wealth value
      for (i in 1:n_obs) {
        asset_idx[i + 1, ] <- asset_idx[i, ] * (1 + asset_ret[i, ])
        asset_wgt[i, ] <- asset_idx[i, ] / sum(asset_idx[i, ])
        if (rebal_counter <= length(rebal_dt) & 
            rebal_dt[rebal_counter] <= ret_dt[i]) {
          asset_idx[i + 1, ] <- as.numeric(sum(asset_idx[i, ])) *
            as.numeric(rebal_wgt[rebal_counter, ]) * 
            as.numeric((1 + asset_ret[i, ]))
          asset_wgt[i, ] <- rebal_wgt[rebal_counter, ]
          rebal_counter <- rebal_counter + 1
        }
      }
      asset_wgt <- xts(asset_wgt, zoo::index(asset_ret))
      dt_rng <- c(ret_dt[1] - 1, ret_dt)
      port_wealth <- xts(rowSums(asset_idx), dt_rng)
      colnames(port_wealth) <- self$name
      # contr to ret = portfolio wealth at end of last period *
      # weight at beginning of current period * current period return
      # port_wealth is lagged, so i = last period
      ctr_mat <- matrix(nrow = n_obs, ncol = ncol(asset_ret))
      for (i in 1:n_obs) {
        ctr_mat[i, ] <- as.numeric(port_wealth[i]) * asset_wgt[i, ] *
          asset_ret[i, ]
      }
      ctr_mat <- xts(ctr_mat, zoo::index(asset_ret))
      pr <- price_to_ret(port_wealth)
      colnames(pr) <- self$name
      self$rebal_ret <- pr
      self$ctr_mat <- ctr_mat
    }
  )
)