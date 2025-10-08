#' @export
tbl_mpt <- function(res, freq = "days") {
  if (inherits(res$b, "xts")) {
    if (ncol(res$b) == 0) {
      is_bench <- FALSE
    } else {
      is_bench <- TRUE
    }
  } else {
    is_bench <- FALSE
  }
  a <- freq_to_scaler(freq)
  xcov <- cov(res$xb) * a
  if (nrow(res$xb) >= a) {
    geo_ret <- calc_geo_ret(res$xb, freq)
    rf_geo <- calc_geo_ret(res$rf, freq)
  } else {
    geo_ret <- apply(res$xb + 1, 2, prod) - 1
    rf_geo <- prod(1+res$rf) - 1
  }
  vol <- sqrt(diag(xcov))
  down_vol <- calc_down_vol(res$xb, freq)
  max_dd <- calc_max_drawdown(res$xb)
  sharpe <- calc_sharpe_ratio(res$xb, res$rf, freq)
  sortino <- calc_sortino_ratio(res$xb, res$rf, freq)
  recov <- geo_ret / -max_dd
  per_up <- apply(res$xb, 2, \(x){sum(x >= 0) / length(x)})
  per_down <- 1 - per_up
  avg_up <- apply(res$xb, 2, \(x){mean(x[x >= 0])})
  avg_down <- apply(res$xb, 2, \(x){mean(x[x <0])})
  res_out <- list()
  if (!is_bench) {
    x <- rbind(
      scales::percent(geo_ret, 0.01),
      scales::percent(vol, 0.01),
      scales::percent(down_vol, 0.01),
      scales::percent(max_dd, 0.01),
      scales::number(sharpe, 0.01),
      scales::number(sortino, 0.01),
      scales::number(recov, 0.01),
      scales::percent(per_up, 0.01),
      scales::percent(per_down, 0.01),
      scales::percent(avg_up, 0.01),
      scales::percent(avg_down, 0.01),
      scales::number(avg_up / -avg_down, 0.01),
      scales::number((avg_up * per_up) / -(avg_down * per_down), 0.01)
    )
    num <- rbind(geo_ret, vol, down_vol, max_dd, sharpe, sortino, recov,
                 per_up, per_down, avg_up, avg_down, avg_up / -avg_down,
                 (avg_up * per_up) / -(avg_down * per_down))
    xdf <- data.frame(
      Metric = c("Geometric Return", "Volatility", "Downside Vol",
                 "Worst Drawdown", "Sharpe Ratio", "Sortino Ratio", "Recovery",
                 "Up Periods", "Down Periods", "Average Up Period",
                 "Average Down Period", "Avg Up / Avg Down",
                 "Wgt Avg Up / Wgt Avg Down"),
      x,
      row.names = NULL
    )
    colnames(xdf) <- c("Metric", colnames(res$xb))
    numdf <- data.frame(
      Metric = c("Geometric Return", "Volatility", "Downside Vol",
                 "Worst Drawdown", "Sharpe Ratio", "Sortino Ratio", "Recovery",
                 "Up Periods", "Down Periods", "Average Up Period",
                 "Average Down Period", "Avg Up / Avg Down",
                 "Wgt Avg Up / Wgt Avg Down"),
      num,
      row.names = NULL
    )
    colnames(numdf) <- c("Metric", colnames(res$xb))
    res_out$xdf <- xdf
    res_out$numdf <- numdf
    return(res_out)
  } else {
    ar <- excess_ret(res$x, res$b)
    acov <- cov(ar) * a
    te <- c(sqrt(diag(acov)), NA)
    xbeta <- calc_uni_beta(res$x, res$b, res$rf)
    up_capt <- c(calc_up_capture(res$x, res$b, freq), NA)
    down_capt <- c(calc_down_capture(res$x, res$b, freq), NA)
    bat_avg <- c(apply(ar, 2, \(x){sum(x >= 0) / length(x)}), NA)
    act_ret <- geo_ret - geo_ret[length(geo_ret)]
    act_ret[length(act_ret)] <- NA
    ir <- act_ret / te
    ir[length(ir)] <- NA
    xcor <- cor(res$xb)[ncol(res$xb), ]
    xalpha <- (geo_ret - rf_geo) - (geo_ret[length(geo_ret)] - rf_geo) * xbeta
    xalpha[length(xalpha)] <- NA
    treynor <- geo_ret / xbeta
    treynor[length(treynor)] <- NA
    max_te_dd <- c(calc_max_drawdown(ar), NA)
    x <- rbind(
      scales::percent(geo_ret, 0.01),
      scales::percent(act_ret, 0.01),
      scales::percent(vol, 0.01),
      scales::number(xbeta, 0.01),
      scales::percent(te, 0.01),
      scales::percent(down_vol, 0.01),
      scales::percent(max_dd, 0.01),
      scales::percent(max_te_dd, 0.01),
      scales::number(sharpe, 0.01),
      scales::number(treynor, 0.01),
      scales::number(ir, 0.01),
      scales::number(sortino, 0.01),
      scales::number(recov, 0.01),
      scales::number(act_ret / -max_te_dd, 0.01),
      scales::percent(up_capt, 0.01),
      scales::percent(down_capt, 0.01),
      scales::number(up_capt / down_capt, 0.01),
      scales::percent(bat_avg, 0.01),
      scales::percent(xalpha, 0.01),
      scales::percent(xcor, 0.01),
      scales::percent(xcor^2, 0.01),
      scales::percent(per_up, 0.01),
      scales::percent(per_down, 0.01),
      scales::percent(avg_up, 0.01),
      scales::percent(avg_down, 0.01),
      scales::number(avg_up / -avg_down, 0.01),
      scales::number((avg_up * per_up) / -(avg_down * per_down), 0.01)
    )
    xdf <- data.frame(
      Metric = c("Geometric Return", "Active Return", "Volatility",
                 "Beta", "Tracking Error", "Downside Vol", "Worst Drawdown",
                 "Worst Active Drawdown", "Sharpe Ratio", "Treynor Ratio",
                 "Info Ratio", "Sortino Ratio", "Recovery Ratio",
                 "Active Recovery Ratio", "Up Capture", "Down Capture",
                 "Up Capt / Down Capt",
                 "Batting Average", "Alpha", "Correlation", "R-squared",
                 "Up Periods", "Down Periods", "Average Up Period",
                 "Average Down Period", "Avg Up / Avg Down",
                 "Wgt Avg Up / Wgt Avg Down"),
      x,
      row.names = NULL
    )
    colnames(xdf) <- c("Metric", colnames(res$xb))
    num <- rbind(geo_ret, act_ret, vol, xbeta, te, down_vol, max_dd, max_te_dd,
                 sharpe, treynor, ir, sortino, recov, act_ret / -max_te_dd,
                 up_capt, down_capt, up_capt / down_capt, bat_avg, xalpha,
                 xcor, xcor^2, per_up, per_down, avg_up, avg_down,
                 avg_up / -avg_down, (avg_up * per_up) / -(avg_down * per_down))
    numdf <- data.frame(
      Metric = c("Geometric Return", "Active Return", "Volatility",
                 "Beta", "Tracking Error", "Downside Vol", "Worst Drawdown",
                 "Worst Active Drawdown", "Sharpe Ratio", "Treynor Ratio",
                 "Info Ratio", "Sortino Ratio", "Recovery Ratio",
                 "Active Recovery Ratio", "Up Capture", "Down Capture",
                 "Up Capt / Down Capt",
                 "Batting Average", "Alpha", "Correlation", "R-squared",
                 "Up Periods", "Down Periods", "Average Up Period",
                 "Average Down Period", "Avg Up / Avg Down",
                 "Wgt Avg Up / Wgt Avg Down"),
      num,
      row.names = NULL
    )
    colnames(numdf) <- c("Metric", colnames(res$xb))
    res_out$xdf <- xdf
    res_out$numdf <- numdf
    return(res_out)
  }
}

tbl_port_wgt <- function(rv, freq = "days") {
  xcov <- cov(rv$ret$x) * freq_to_scaler(freq)
  x <- rv$port_tbl$Weight
  if (rv$sum_to_1) {
    x <- x / sum(x)
  }
  vol_wgt <- risk_wgt(x, xcov)
  p <- Rebal$new(x, rv$ret$x)
  p$rebal()
  dd <- find_drawdowns(p$rebal_ret)
  dd_sort <- dd[order(dd$Drawdown), ]
  dd_ctr <- contr_to_ret(p, dd_sort$StartDate[1], dd_sort$TroughDate[1])
  dd_wgt <- dd_ctr / sum(dd_ctr)
  res <- data.frame(
    Asset = rv$port_tbl$Asset,
    Cap.Weight = scales::percent(x, 0.01),
    Vol.Weight = scales::percent(vol_wgt, 0.01),
    Drawdown.Weight = scales::percent(dd_wgt, 0.01)
  )
  tot_rows <- data.frame(
    Asset = c("Total Weight", "Total Stat"),
    Cap.Weight = c(scales::percent(1, 0.01), NA),
    Vol.Weight = scales::percent(c(1, port_risk(x, xcov)), 0.01),
    Drawdown.Weight = scales::percent(c(1, dd_sort$Drawdown[1]), 0.01)
  )
  if (inherits(rv$bench, "xts")) {
    dd <- find_drawdowns(p$rebal_ret - rv$ret$b)
    dd_sort <- dd[order(dd$Drawdown), ]
    dd_ctr <- p$contr_to_ret(dd_sort$StartDate[1], dd_sort$TroughDate[1])
    rel_dd_wgt <- dd_ctr / sum(dd_ctr)
    res$Active.Drawdown.Weight <- scales::percent(rel_dd_wgt, 0.01)
    tot_rows$Active.Drawdown.Weight <-
      scales::percent(c(1, dd_sort$Drawdown[1]), 0.01)
  }
  res <- rbind(res, tot_rows)
  colnames(res) <- gsub("\\.", " ", colnames(res))
  return(res)
}

viz_rel_perf_cone <- function(x, b, freq = "days", mu = NULL, sigma = NULL) {
  r <- x - b
  if (is.null(sigma)) {
    sigma <- sd(r)
  } else {
    sigma <- sigma / sqrt(freq_to_scaler(freq))
  }
  if (is.null(mu)) {
    mu <- 0
  } else {
    mu <- mu / freq_to_scaler(freq) - 1/2 * sigma * sigma
  }
  p <- ret_to_price(r) - 1
  n_obs <- nrow(r) + 1
  mu_line <- 1:n_obs * mu
  sigma_line <- sqrt(1:n_obs) * sigma
  xdf <- data.frame(
    Date = zoo::index(p),
    Perf = p,
    Mu = mu_line,
    Up.1 = mu_line + sigma_line,
    Up.2 = mu_line + 2 * sigma_line,
    Down.1 = mu_line - sigma_line,
    Down.2 = mu_line - 2 * sigma_line
  )
  tdf <- pivot_longer(xdf, -Date)
  tdf$value <- exp(tdf$value)
  ggplot(tdf, aes(x = Date, y = value, color = name)) +
    geom_line() +
    scale_y_continuous(trans = "log")
}

contr_to_ret = function(p, date_start = NULL, date_end = NULL) {
  date_start <- as.Date(date_start)
  date_end <- as.Date(date_end)
  ctr_mat <- cut_time(p$ctr_mat, date_start, date_end)
  if (!is.null(date_start)) {
    pw <- prod(1 + cut_time(p$rebal_ret, date_end = date_start -1)) *
      100
  } else {
    pw <- 100
  }
  colSums(ctr_mat) / pw
}

#' @export
viz_wealth_index <- function (x, init_val = 100) {
  wi <- ret_to_price(x) * init_val
  dat <- xts_to_tidy(wi)
  dat$name <- factor(dat$name, unique(dat$name))
  col <- set_plot_col(ncol(x))
  col <- unname(col)
  ggplot(dat, aes(x = Date, y = value, color = name)) + geom_line() +
    scale_color_manual(values = col) +
    scale_y_continuous(labels = scales::number) +
    ylab("") + labs(color = "", title = "Cumulative Wealth") +
    theme_light()
}
