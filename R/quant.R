# splines ----
#' @title Spline Quarterly Returns with Monthly Returns
#' @param m_ret monthly returns xts
#' @param q_ret quarterly returns xts
#' @param thresh epsilon of difference between return series, see details
#' @param details
#'    The monthly time-series is adjusted by a constant each quarter so it
#'    its compounded return exactly equals (less epsilon) the compounded return
#'    of the quarterly time-series. A good use case is splicing quarterly
#'    Private Equity TWR with the Russell 2000 monthly TWR.
#' @export
monthly_spline <- function(m_ret, q_ret, thresh = 0.00001) {
  q_dt <- zoo::index(q_ret)
  if (floor_date(q_dt)[1] < zoo::index(m_ret)[1]) {
    warning("quarterly time-series starts before monthly")
    q_dt <- q_dt[q_dt > zoo::index(m_ret)[1]]
    if (length(q_dt) == 0) {
      stop("no overlapping dates found")
    }
  }
  m_adj <- m_ret
  for (i in 1:length(q_dt)) {
    q_start <- floor_date(q_dt[i], "quarters")
    rng <- paste0(q_start, "/", q_dt[i])
    m_ret_i <- m_ret[rng]
    q_ret_i <- q_ret[q_dt[i]]
    lx <- -0.9
    ux <- 0.95
    for (j in 1:1000) {
      mx <- (lx + ux) / 2
      if (abs(test_diff(m_ret_i + mx, q_ret_i)) < thresh) {
        break
      }
      if (test_diff(m_ret_i + mx, q_ret_i) > 0) {
        ux <- mx
      } else {
        lx <- mx
      }
    }
    m_adj[rng] <- m_ret[rng] + mx
  }
  return(m_adj)
}

#' @title Splice a monthly time-series with a daily time-series.
#' @param d_ret daily returns xts
#' @param m_ret monthly returns xts
#' @param thresh epsilon of difference between return series, see details
#' @param details
#'    The daily time-series is adjusted by a constant each month so it
#'    its compounded return exactly equals (less epsilon) the compounded return
#'    of the monthly time-series. A good use case is splicing monthly CTF
#'    returns with daily estimates.
#' @export
daily_spline <- function(d_ret, m_ret, thresh = 0.0000001) {
  d_ret <- clean_ret(d_ret, eps = 1)
  m_ret <- clean_ret(m_ret, eps = 1)
  d_ret <- d_ret$ret
  m_ret <- m_ret$ret
  d_incept <- first_comm_start(d_ret)
  m_ret <- cut_time(m_ret, d_incept)
  m_dt <- zoo::index(m_ret)
  m_dt <- substr(m_dt, 1, 7)
  d_adj <- d_ret
  for (m in 1:nrow(m_ret)) {
    dt_ix <- paste0(m_dt[m], '/', m_dt[m])
    di <- d_ret[dt_ix]
    mi <- m_ret[m_dt[m]]
    lx <- -0.01
    ux <- 0.011
    for (i in 1:1000) {
      mx <- (lx + ux) / 2
      if (abs(test_diff(di + mx, mi)) < thresh) {
        break
      }
      if (test_diff(di + mx, mi) > 0) {
        ux <- mx
      } else {
        lx <- mx
      }
    }
    d_adj[dt_ix] <- d_ret[dt_ix] + mx
  }
  return(d_adj)
}

#' @title Utility function for testing the difference of the product of A and B
#' @export
test_diff <- function(a, b) {
  (prod(a+1)-1) - b
}

#' @title Unsmooth serially auto-correlated returns
#' @param x return xts to unsmooth
#' @note
#' Ru = R - R_lag * rho / (1 - rho)
#' Ru = unsmoothed return
#' R = return
#' R_lag = one period lagged return
#' rho = correlation b/t R and R_lag
#' @export
unsmooth_ret <- function(x) {
  lag_x <- lag.xts(x)
  rho <- as.numeric(acf(as.numeric(x), plot = FALSE)[1][[1]])
  na.omit((x - lag_x * rho) / (1 - rho))
}

# Port calcs ----

#' @title Calculate Volatility or TE Weights
#' @param x vector of weights corresponding to `cov_mat`
#' @param cov_mat covariance matrix
#' @return a vector of risk weights
#' @details
#' For TE `x` is a vector of active weights (weight - benchmark weight) and
#' the `cov_mat` is a union of the portfolio and benchmark positions that
#' corresponds to the active weight vector `x`
#' @export
risk_wgt <- function(x, cov_mat) {
  wgt <- matrix(x, ncol = 1)
  (wgt * (cov_mat %*% wgt)) / (t(wgt) %*% cov_mat %*% wgt)[1]
}


#' @title Calculate Factor Risk Weight
#' @param x_beta factor betas from regression
#' @param x_cov covariance matrix of the regression factors
#' @param y_variance variance of the y variable
#' @return a vector of the factor risk weights
#' @details
#' Risk weights are the contribution to the total variance of the y variable,
#' i.e., the fund or asset to explain
#' @export
factor_risk_wgt <- function(x_beta, x_cov, y_variance) {
  xbeta <- matrix(xbeta, nrow = length(x_beta), ncol = 1)
  (x_beta * (x_cov %*% x_beta)) / y_variance[1]
}


#' @title Portfolio Volatility
#' @param x vector of weights
#' @param cov_mat covariance matrix
#' @export
port_risk <- function(x, cov_mat) {
  wgt <- matrix(x, ncol = 1)
  sqrt((t(wgt) %*% cov_mat %*% wgt)[1])
}


#' @title Portfolio Return
#' @param x vector of weights
#' @param mu_vec vector of expected or historic means
#' @export
port_ret <- function(x, mu_vec) {
  wgt <- matrix(x, ncol = 1)
  mu <- matrix(mu_vec, ncol = 1)
  t(wgt) %*% mu
}

#' @title Correlation between two portfolios
#' @param wgt_vec_1 weight vector of 1st portfolio
#' @param wgt_vec_2 weight vector of 2nd portfolio
#' @param cov_mat union covariance of wgt_vec_1 and wgt_vec_2
#' @export
port_corr <- function(wgt_vec_1, wgt_vec_2, cov_mat) {
  w1 <- as.matrix(wgt_vec_1, ncol = 1)
  w2 <- as.matrix(wgt_vec_2, ncol = 1)
  (t(w1) %*% cov_mat %*% w2) /
    (sqrt(t(w1) %*% cov_mat %*% w1) * sqrt(t(w2) %*% cov_mat %*% w2))
}

#' @title Beta between two portfolios
#' @param wgt_vec_1 weight vector of 1st portfolio
#' @param wgt_vec_2 weight vector of 2nd portfolio
#' @param cov_mat union covariance of wgt_vec_1 and wgt_vec_2
#' @export
port_beta <- function(wgt_vec_1, wgt_vec_2, cov_mat) {
  w1 <- as.matrix(wgt_vec_1, ncol = 1)
  w2 <- as.matrix(wgt_vec_2, ncol = 1)
  (t(w2) %*% cov_mat %*% w1) / (t(w2) %*% cov_mat %*% w2)
}

#' @title Calculate Risk Parity Weights
#' @param ret xts of returns
#' @return vector of risk parity weights
#' @examples
#' w <- risk_par_wgt(x)
#' w
#' risk_wgt(w, cov(x))
#' @export
risk_par_wgt <- function(ret) {
  sigma <- cov(ret)
  # F(y), system of nonlinear equations
  eval_f <- function(x, sigma, lamda) {
    x <- as.vector(x)
    x <- x[1:nrow(sigma)]
    f0 <- matrix(nrow = (nrow(sigma) + 1), ncol = 1)
    f0[1:nrow(sigma), 1] <- (sigma %*% x) - (lamda * 1 / x)
    f0[(nrow(sigma) + 1), 1] <- sum(x) - 1
    return(f0)
  }
  # Jacobian matrix of F(y)
  jacob_f <- function(x, sigma, lamda) {
    x <- as.vector(x)
    x <- x[1:nrow(sigma)]
    g <- matrix(nrow = (nrow(sigma) + 1), ncol = (ncol(sigma) + 1))
    g[1:nrow(sigma), 1:ncol(sigma)] <- sigma + as.vector(lamda) *
      diag(1 / x^2)
    g[1:nrow(sigma), (ncol(sigma) + 1)] <- 1 / x
    g[(nrow(sigma) + 1), 1:ncol(sigma)] <- rep(1, ncol(sigma))
    g[(nrow(sigma) + 1), (ncol(sigma) + 1)] <- 0
    return(g)
  }
  # set stopping points for max iterations if no solution
  # and tolerance for solution
  max_iter <- 100
  tol <- 1e-6
  # establish list for ERP weights to go into and set
  # number of assets
  n_assets <- ncol(ret)
  n_obs <- nrow(ret)
  for(i in 1:max_iter){
    if (i == 1){
      # 1/N first weight guess
      next_x <- c(rep(1/n_assets, n_assets), 1)
      lamda <- t(next_x[1:n_assets]) %*% sigma %*%
        next_x[1:n_assets] / n_assets
    }
    else{
      j_inv <- solve(jacob_f(next_x, sigma, lamda))
      f_mat <- eval_f(next_x, sigma, lamda)
      ans <- next_x - j_inv %*% f_mat
      if(norm(next_x - ans) <= tol){
        break
      }
      next_x <- ans
      lamda <- t(next_x[1:n_assets,]) %*% sigma %*%
        next_x[1:n_assets,] / n_assets
    }
  }
  return(ans[1:n_assets])
}

#' @title Absorption Ratio
#' @param xcor correlation matrix
#' @param n_pc number of PCs for calculation, see details
#' @details From Kritzman, et al (2010), sum(1:n_pc) / sum(all pcs)
#' @export
absorp_ratio <- function(xcor, n_pc = NULL) {
  if (is.null(n_pc)) {
    n_pc <- ceiling(nrow(xcor) / 5)
  }
  eig_res <- eigen(xcor)
  sum(eig_res$values[1:2]) / sum(eig_res$values)
}

# MPT calcs ----

calc_var <- function(x, p = 0.05, method = c("boot", "stable")) {
  method <- tolower(method)[1]
  if (method == "stable") {
    sfit <- sapply(x, stable_fit_mle)
    res <- apply(sfit, 2, \(x) {stable_q(p = p, pars = x)})
  } else if (method == "boot") {
    res <- apply(x, 2, \(x) {sort(sample(x, 1000, TRUE),
                                  decreasing = FALSE)[round(p * 1000, 0)]})
  } else {
    stop("invalid method")
  }
  return(res)
}

#' @title Calculate Peak to Trough Drawdowns
#' @param x xts object
#' @return xts of drawdown time-series
#' @export
calc_drawdown <- function(x) {
  x <- na.omit(x)
  dd <- apply(x, 2, .drawdown)
  xts(dd, as.Date(rownames(dd), origin = '1970-01-01'))
}

#' @title Drawdown utility function for calc_drawdown
#' @param x xts object
#' @export
.drawdown <- function(x) {
  wi <- cumprod(x + 1)
  wi_peak <- cummax(wi)
  wi / wi_peak - 1
}

#' @title Calculate Worst Drawdown
#' @param x xts
#' @return vector of worst drawdowns for each column in `x`
#' @export
calc_max_drawdown <- function(x) {
  dd <- calc_drawdown(x)
  apply(dd, 2, min)
}

#' @title Find all the drawdowns of a time-series
#' @param x univariate xts, if more than one column is passed only the first
#'   column will be used
#' @return data.table with all the drawdowns, fields include StartDate,
#'    EndDate (recovery), TroughDate, Trough, DaysToTrough, DaysToRecover, and
#'    TotalDays
#' @export
find_drawdowns <- function(x) {
  if (ncol(x) > 1) {
    warning('x needs to be univariate, taking the first column')
    x <- x[, 1]
  }
  dd <- calc_drawdown(x)
  dd <- xts_to_dataframe(dd)
  colnames(dd) <- c('Date', 'Drawdown')
  dd$isDown <- dd[, 2] < 0
  dd$isDownLag <- c(NA, dd[1:(nrow(dd) - 1), 'isDown'])
  # change from 0 to negative signals drawdown start
  dd$start <- dd$isDown & !dd$isDownLag
  # change from negative back to 0 signals recovery
  dd$end <- !dd$isDown & dd$isDownLag
  start_date <- dd[dd$start, 1]
  end_date <- dd[dd$end, 1]
  # if lengths of start and end dates are the same the time-series ends on
  # a drawdown and we need to adjust last end date to last start date
  if (length(end_date) == length(start_date)) {
    dd_date <- data.frame(StartDate = start_date,
                          EndDate = c(end_date[2:length(end_date)],
                                      dd$Date[nrow(dd)]))
  } else {
    dd_date <- data.frame(StartDate = start_date,
                          EndDate = end_date[2:length(end_date)])
  }

  .trunc_df <- function(df, date_start = NULL, date_end = NULL) {

    colnames(df)[1] <- 'Date'
    if (!is.null(date_start)) {
      ind <- df$Date >= as.Date(date_start)
      df <- df[ind, ]
    }
    if (!is.null(date_end)) {
      ind <- df$Date <= as.Date(date_end)
      df <- df[ind, ]
    }
    return(df)
  }
  # create unique list of drawdowns
  dd_list <- mapply(.trunc_df,
                    df = list(dd[, 1:2]),
                    date_start = dd_date$StartDate,
                    date_end = dd_date$EndDate,
                    SIMPLIFY = FALSE)
  .get_min_dd <- function(x) {
    # find trough by finding which observation equals the minimum
    indx <- which(x[, 2] == min(x[, 2]))
    # if there are more than one observations equal to the minimum take the
    # first one
    if (length(indx) > 1) {
      indx <- indx[1]
    }
    x[indx, ]
  }
  trough_list <- lapply(dd_list, .get_min_dd)
  trough_df <- do.call(rbind, trough_list)
  res <- data.frame(StartDate = dd_date$StartDate,
                    TroughDate = trough_df$Date,
                    EndDate = dd_date$EndDate,
                    Trough = trough_df$Drawdown,
                    DaysToTrough = trough_df$Date - dd_date$StartDate,
                    DaysToRecover = dd_date$EndDate - trough_df$Date,
                    TotalDays = dd_date$EndDate - dd_date$StartDate)
  colnames(res)[4] <- 'Drawdown'
  return(res)
}

#' @title Calculate excess return (x - b)
#' @param x xts object to transform into excess return
#' @param b univariate xts object of benchmark or risk-free rate to subtract
#'   from `x`
#' @export
excess_ret <- function(x, b) {
  x - b[, rep(1, ncol(x))]
}

#' @title Calculate geometric return
#' @param x xts object
#' @param freq days, weeks, months, quarters to annualize return
#' @return vector of geometric returns
#' @export
calc_geo_ret <- function(x, freq) {
  a <- freq_to_scaler(freq)
  obs <- nrow(x)
  r <- apply(x + 1, 2, prod)
  if (obs > a) {
    return(r^(a / obs) - 1)
  } else {
    return(r - 1)
  }
}

#' @title Calculate Volatiltiy
#' @param x xts object
#' @param freq days, weeks, months, quarters to annualize volatiltiy
#' @return vector of volatility
#' @export
calc_vol <- function(x, freq) {
  a <- freq_to_scaler(freq)
  apply(x, 2, sd) * sqrt(a)
}


#' @title Calculate Sharpe Ratio
#' @param x xts object
#' @param rf xts with risk-free time-series
#' @param freq days, weeks, months, quarters to annaulize return and risk
#' @note
#' The function doesn't test to see if `x` and `rf` are for intersecting
#' time freqs.
#' @return vector of Sharpe Ratios
#' @export
calc_sharpe_ratio <- function(x, rf, freq) {
  x_geo <- calc_geo_ret(x, freq)
  rf_geo <- calc_geo_ret(rf, freq)
  x_vol <- calc_vol(x, freq)
  (x_geo - rf_geo) / x_vol
}


#' @title Calculate Downside Volatiltiy
#' @param x xts object
#' @param freq days, weeks, months, quarters to annualize volatility
#' @param mar default `0`, sets min acceptable return to subset downside
#'   volatility
#' @details
#' Downside volatiltiy is the standard devation of returns for the sub-set of
#' the time-series below the `mar`
#' @return vector of downside volatility
#' @export
calc_down_vol <- function(x, freq, mar = 0) {
  a <- freq_to_scaler(freq)
  res <- rep(NA, ncol(x))
  for (i in 1:ncol(x)) {
    res[i] <- sd(x[x[, i] < mar, i]) * sqrt(a)
  }
  return(res)
}

#' @title Calculate Sortino Ratio
#' @param x xts object
#' @param rf xts with risk-free time-series
#' @param mar default `0`, sets min acceptable return to subset downside
#'   volatility
#' @param freq days, weeks, months, quarters to annaulize return and risk
#' @note
#' The function doesn't test to see if `x` and `rf` are for intersecting
#' time freqs.
#' @return vector of Sortino Ratios
#' @export
calc_sortino_ratio <- function(x, rf, freq, mar = 0) {
  x_geo <- calc_geo_ret(x, freq)
  rf_geo <- calc_geo_ret(rf, freq)
  x_vol <- calc_down_vol(x, freq, mar)
  (x_geo - rf_geo) / x_vol
}


#' @title Calculate Batting Average
#' @details
#' Average up day / Average down day, or week, month, etc
#' @param x xts
#' @param mar sets place to sub-set up / down, default is `0`
#' @export
calc_batting_avg <- function(x, mar = 0) {
  x_up <- rep(NA, ncol(x))
  x_down <- x_up
  for (i in 1:ncol(x)) {
    x_up[i] <- mean(x[x[, i] >= mar, i])
    x_down[i] <- mean(x[x[, i] < mar, i])
  }
  x_up / abs(x_down)
}


#' @title Calculate Upside Capture
#' @details
#' Geometric return of manager / geometric return of benchmark when benchmark
#'   is up
#' @param mgr xts object of manager / asset / security / etc to calculate
#'   upside capture
#' @param bench xts object of benchmark
#' @param mar sets place to sub-set up / down, default is `0`
#' @return vector of upside capture
#' @export
calc_up_capture <- function(mgr, bench, freq, mar = 0) {
  mgr_up <- mgr[bench >= mar, ]
  bench_up <- bench[bench >= mar, ]
  mgr_geo <- colMeans(mgr_up)
  bench_geo <- colMeans(bench_up)
  mgr_geo / bench_geo
}


#' @title Calculate Downside Capture
#' @details
#' Geometric return of manager / geometric return of benchmark when benchmark
#'   is down
#' @param mgr xts object of manager / asset / security / etc to calculate
#'   downside capture
#' @param bench xts object of benchmark
#' @param mar sets place to sub-set up / down, default is `0`
#' @return vector of downside capture
#' @export
calc_down_capture <- function(mgr, bench, freq, mar = 0) {
  mgr_down <- mgr[bench < mar, ]
  bench_down <- bench[bench < mar, ]
  mgr_geo <- colMeans(mgr_down)
  bench_geo <- colMeans(bench_down)
  mgr_geo / bench_geo
}


#' @title Calculate Univariate Beta
#' @param mgr xts of manager(s) returns
#' @param bench xts of benchmark returns
#' @param rf xts of risk-free returns
#' @return vector of betas to benchmark
#' @export
calc_uni_beta <- function(mgr, bench, rf) {
  combo <- cbind(mgr, bench)
  er <- excess_ret(combo, rf)
  xcov <- cov(er)
  # covariance / variance
  xcov[, ncol(xcov)] / xcov[nrow(xcov), ncol(xcov)]
}


#' @title Calculate Beta to benchmark when benchmark is up
#' @param mgr xts of manager(s) returns
#' @param bench xts of benchmark returns
#' @param rf xts of risk-free returns
#' @param mar numeric minimal acceptable return for up / down split
#' @export
up_beta <- function(mgr, bench, rf, mar = 0) {
  is_up <- bench >= mar
  calc_uni_beta(mgr[is_up, ], bench[is_up, ], rf[is_up, ])
}

#' @title Calculate Beta to benchmark when benchmark is down
#' @param mgr xts of manager(s) returns
#' @param bench xts of benchmark returns
#' @param rf xts of risk-free returns
#' @param mar numeric minimal acceptable return for up / down split
#' @export
down_beta <- function(mgr, bench, rf, mar = 0) {
  is_down <- bench < mar
  calc_uni_beta(mgr[is_down, ], bench[is_down, ], rf[is_down, ])
}

#' @title Exponentially Weighted Moving Average Covariance
#' @param ret xts of returns
#' @param lamda numeric value to scale weighted average calc
#' @note
#' If lamda is left to default NULL it is calculated as 1 - 2 / a, where a
#' = the periodicity scaler (e.g., months = 12)
#' @export
cov_ewma <- function(ret, lamda = NULL) {
  if (is.null(lamda)) {
    freq <- xts::periodicity(ret)
    lamda <- 1 - 2 / (freq_to_scaler(freq$units))
  }
  n_obs <- nrow(ret)
  cov_mat <- cov(ret)
  mu <- colMeans(ret)
  ret_centered <- sweep(ret, 2, mu, '-')
  for (obs in 1:n_obs) {
    r <- ret_centered[obs, ]
    rr <- t(r) %*% r
    cov_mat <- (1 - lamda) / (1 + lamda^n_obs) * rr + lamda * cov_mat
  }
  return(cov_mat)
}

# min TE ----

#' @title Quadratic solver wrapper for minimizing tracking error
#' @param fund xts of asset to replicate / minimize TE to
#' @param fact xts of factors / assets to use to try and replicate `fund`
#' @return results from `quadprog::solve.QP`
#' @export
te_min_qp <- function(fund, fact) {

  n_fact <- ncol(fact)
  cov_fact <- cov(fact)
  cov_vec <- matrix(nrow = n_fact, ncol = 1)
  for (i in 1:n_fact) {
    cov_vec[i, 1] <- cov(fact[, i], fund)
  }
  a_mat_t <- rbind(rep(1, n_fact), diag(-1, n_fact), diag(1, n_fact))
  a_mat <- t(a_mat_t)
  b_0 <- c(1, rep(-1, n_fact), rep(0, n_fact))
  res <- quadprog::solve.QP(cov_fact, cov_vec, a_mat, bvec = b_0, meq = 1)
  return(res)
}



# roll ----

#' @title Rolling Beta to Common Benchmark
#' @param x xts of asset returns
#' @param b xts of benchmark returns (one benchmark)
#' @param rf xts of risk-free
#' @param n number of periods for rolling window
#' @details
#' Trailing beta to benchmark is calculated for `n` periods. If the data
#' are monthly then `n` set to 36 is 36 months. The rolling period includes
#' the current period: e.g., for monthly returns with `n` = 3 on June will
#' include April, May, and June returns.
#' @export
roll_beta <- function(x, b, rf, n) {
  if (ncol(b) > 1) {
    b <- b[, 1]
    warning('more than one benchmark entered, only taking first column')
  }
  if (ncol(rf) > 1) {
    rf <- rf[, 1]
    warning('more than one rf entered, only taking first column')
  }
  combo <- clean_asset_bench_rf(x, b, rf)
  x_er <- combo$x - combo$rf[, rep(1, ncol(combo$x))]
  b_er <- combo$b - combo$rf
  obs <- cbind.xts(x_er, b_er, check.names = FALSE)
  obs <- xts_to_dataframe(obs)
  rcov <- slider::slide(obs[, -1, drop = FALSE], ~cov(.x), .before = n-1,
                        .complete = TRUE)
  xbeta <- lapply(rcov, \(x) {x[, ncol(x)] / x[ncol(x), ncol(x)]})
  xbeta <- do.call('rbind', xbeta)
  xts(xbeta, obs$Date[n:nrow(obs)])
}

#' @title Rolling R-squared to Common Benchmark
#' @param x xts of asset returns
#' @param b xts of benchmark return (one benchmark)
#' @param n number of periods for rolling window.
#' @details
#' Trailing r^2 to benchmark is calculated for `n` periods. If the data
#' are monthly then `n` set to 36 is 36 months. The rolling period includes
#' the current period: e.g., for monthly returns with `n` = 3 on June will
#' include April, May, and June returns.
#' @export
roll_r2 <- function(x, b, n) {
  if (ncol(b) > 1) {
    b <- b[, 1]
    warning('more than one benchmark entered, only taking first column')
  }
  combo <- clean_asset_bench_rf(x, b)
  obs <- xts_to_dataframe(cbind.xts(combo$x, combo$b, check.names = FALSE))
  rcor <- slider::slide(obs[, -1, drop = FALSE], ~cor(.x), .before = n-1,
                        .complete = TRUE)
  rcor <- lapply(rcor, \(x) {x[, nrow(x)]})
  xcor <- do.call("rbind", rcor)
  xts(xcor^2, obs$Date[n:nrow(obs)])
}

#' @title Rolling trailing returns
#' @param x xts of returns
#' @param n number of periods for rolling calc window, see details
#' @param b optional benchmark to transform to active return
#' @param freq string: days, weeks, months, quarters, years
#' @details
#' If frequency is "months" and n = 12, a rolling 12 month return is
#' calculated. Note frequency will not change the frequency, it should
#' match the return periodicity. Frequency is used to determine if the
#' returns should be annualized, i.e., if the period is longer than 1 year.
#' Rolling return periods for less than 1 year will not be annualized.
#' The rolling period includes the current period: e.g.,
#' for monthly returns with `n` = 3 on June will include April, May, and
#' June returns.
#' @export
roll_ret <- function(x, n, b = NULL, freq = "days") {
  if (!is.null(b)) {
    x <- excess_ret(x, b)
  }
  obs <- xts_to_dataframe(x)[, -1, drop = FALSE]
  if (n > freq_to_scaler(freq)) {
    rl <- slider::slide(obs, ~calc_geo_ret(.x, freq), .before = n-1,
                        .complete = TRUE)
  } else {
    rl <- slider::slide(obs, ~apply(.x+1, 2, prod)-1, .before = n-1,
                        .complete = TRUE)
  }
  rr <- do.call("rbind", rl)
  xts(rr, zoo::index(x)[n:nrow(x)])
}

#' @title Rolling Volatility
#' @param x xts of returns
#' @param n number of periods for rolling calc window, see details
#' @param b optional benchmark to transform to active return
#' @param freq string: days, weeks, months, quarters, years
#' @details
#' If frequency is "months" and n = 12, a rolling 12 month volatility is
#' calculated. If `b` is specified then the calculation will be the volatility
#' of the active return to the benchmark, i.e., tracking error. Note frequency
#' will not change the frequency, it is used to annaulize the volatility.
#' The rolling period includes the current period: e.g.,
#' for monthly returns with `n` = 3 on June will include April, May, and
#' June returns.
#' @export
roll_vol <- function(x, n, b = NULL, freq = "days") {
  if (!is.null(b)) {
    x <- excess_ret(x, b)
  }
  obs <- xts_to_dataframe(x)[, -1, drop = FALSE]
  rl <- slider::slide(obs, ~calc_vol(.x, freq), .before = n - 1,
                      .complete = TRUE)
  rv <- do.call("rbind", rl)
  xts(rv, zoo::index(x)[n:nrow(x)])
}

#' @title Rolling Downside Volatility
#' @param x xts of returns
#' @param n number of periods for rolling calc window, see details
#' @param b optional benchmark to transform to active return
#' @param freq string: days, weeks, months, quarters, years
#' @details
#' If frequency is "months" and n = 12, a rolling 12 month volatility when
#' the asset is down is calculated. If `b` is specified then the calculation
#' will be the volatility of the active return to the benchmark. Note frequency
#' will not change the frequency, it is used to annaulize the volatility.
#' The rolling period includes the current period: e.g.,
#' for monthly returns with `n` = 3 on June will include April, May, and
#' June returns.
#' @export
roll_down_vol <- function(x, n, b = NULL, freq = "days") {
  if (!is.null(b)) {
    x <- excess_ret(x, b)
  }
  obs <- xts_to_dataframe(x)[, -1, drop = FALSE]
  rl <- slider::slide(obs, ~calc_down_vol(.x, freq), .before = n - 1,
                      .complete = TRUE)
  rv <- do.call("rbind", rl)
  xts(rv, zoo::index(x)[n:nrow(x)])
}

#' @title Rolling Style Analysis
#' @param x xts of asset returns to explain
#' @param b xts of index or factor returns used to track `x`
#' @param n number of observations in rolling period
#' @export
roll_style <- function(x, b, n) {
  combo <- clean_asset_bench_rf(x, b)
  x <- xts_to_dataframe(combo$x)
  b <- xts_to_dataframe(combo$b)
  obs <- cbind(x[,-1, drop = FALSE], b[,-1, drop = FALSE])
  colnames(obs)[1] <- colnames(x)[2]
  r_sty <- slider::slide(obs, ~te_min_qp(.x[, 1], .x[, 2:ncol(obs)])$solution,
                         .before = n-1, .complete = TRUE)
  r_sty <- do.call("rbind", r_sty)
  res <- xts(r_sty, x$Date[n:nrow(x)])
  colnames(res) <- colnames(obs)[-1]
  return(res)
}




#' @title Calculate Weighted Harmonic Mean
#' @param w weight vector
#' @param x data vector
#' @export
wgt_harmonic_mean <- function(w, x) {
  sum(w, na.rm = TRUE) / sum(w / x, na.rm = TRUE)
}

#' @title Calculate Weighted Harmonic Mean of a Ratio in a data.frame
#' @param tbl_hold data.frame
#' @param x column name that contains the ratio
#' @param w column name that contains the weights
#' @details excludes missing and negative ratio and reconstitutes the weights
#' @export
calc_wgt_multiple <- function(tbl_hold, x = "PE", w = "CapWgt") {
  miss <- is.na(tbl_hold[, x])
  tbl_hold <- tbl_hold[!miss, ]
  neg_earn <- tbl_hold[, x] < 0
  tbl_hold <- tbl_hold[!neg_earn, ]
  tbl_hold[, w] <- tbl_hold[, w] / sum(tbl_hold[, w], na.rm = TRUE)
  wgt_harmonic_mean(tbl_hold[, w], tbl_hold[, x])
}

# port ----

#' @title Cluster around latents
#' @param cor_mat correlation matrix
#' @export
pca_hclust <- function(cor_mat) {
  p <- princomp(covmat = cor_mat)
  meas <- diag(p$sdev) %*% t(p$loadings[,])
  dist_res <- dist(t(meas), method = 'euclidean')
  hclust(dist_res)
}


#' @title Calculate risk cluster weights
#' @param hc tree from hclust output
#' @param vol volatility of assets used in hc calculation
#' @param k number of clusters
#' @export
risk_cluster_wgt <- function(hc, vol, k = 2) {

  n_assets <- max(hc$order)
  memb <- cutree(hc, k)
  xcor <- diag(1, n_assets, n_assets)
  for (i in 1:k) {
    xcor[memb == i, memb == i] <- 1
  }
  vol <- matrix(vol, ncol = 1)
  xcov <- vol %*% t(vol) * xcor
  mu_vec <- vol * 0.25
  cov_inv <- MASS::ginv(xcov)
  (cov_inv %*% mu_vec) /
    (matrix(1, ncol = length(hc$order), nrow = 1) %*% cov_inv %*% mu_vec)[1]
}


# MC sim ----

#' @export
boot_strap <- function(x, n) {
  is_vec <- is.null(dim(x))
  if (is_vec) {
    n_row <- length(x)
  } else {
    n_row <- nrow(x)
  }
  rand_row <- round(runif(n, 1, n_row), 0)
  if (is_vec) {
    return(x[rand_row])
  } else {
    return(x[rand_row, ])
  }
}


#' @export
block_boot_strap <- function(x, n, block) {
  x <- as.vector(x)
  boot_length <- ceiling(n / block)
  boot_vec <- rep(NA, block * boot_length)
  rand_row <- round(runif(boot_length, 0, length(x) - block + 1), 0)
  j <- 1
  for (i in 1:boot_length) {
    rand_block <- rand_row[i]:(rand_row[i] + (block - 1))
    boot_vec[j:(j + (block - 1))] <- x[rand_block]
    j <- j + block
  }
  boot_vec[1:n]
}



# find n pcs ----

#' @export
sig_group_sim <- function(ret) {

  xcor <- cor(ret)
  lamda <- eigen(xcor)$values

  noise_lamda <- matrix(nrow = 1000, ncol = length(lamda))
  for (i in 1:1000) {
    noise_ret <- apply(ret, 2, sample, size = nrow(ret), replace = FALSE)
    noise_lamda[i, ] <- eigen(cor(noise_ret))$values
  }
  noise_95 <- apply(noise_lamda, 2, quantile, probs = 0.95)
  return(sum(lamda > noise_95))
}

