#' @export
dtc_col <- function() {
  x <- c(
    "#003057", # navy blue
    "#719E8B", # pine
    "#9B9EA0", # cool gray
    "#CC9F26", # gold
    "#ACCACD", # mist
    "#C06D59", # terra cotta
    "#CFC7C0", # sand
    "#668EA4", # slate blue
    "#D5E4C0", # sage
    "#DE8958", # orange
    "#C1D2C7", # cool green
    "#C55265", # berry
    "#8DA9B4"  # ocean
  )
  names(x) <- c("navyblue", "pine", "coolgray", "gold", "mist", "terracotta",
                "sand", "slateblue", "sage", "orange", "coolgreen", "berry",
                "ocean")
  return(x)
}

#' @export
set_plot_col <- function(n) {
  x <- dtc_col()
  if (n > length(x)) {
    x <- c(x, 1:(n - length(x)))
  }
  return(x)
}


#' @export
run_ppu <- function(bucket, as_of = NULL) {
  if (is.null(as_of)) {
    as_of <- last_us_trading_day(Sys.Date())
  } else {
    as_of <- as.Date(as_of)
  }
  ppu_tbl <- try_read(bucket, "meta-tables/ppu.parquet")
  r <- read_ret(ppu_tbl$DtcName, bucket)
  r <- cut_time(r, date_end = as_of)
  tbl_name <- unique(ppu_tbl$Table)
  res <- list()
  for (i in 1:length(tbl_name)) {
    res[[i]] <- ppu_sect(ppu_tbl, r, as_of, tbl_name[i])
  }
  names(res) <- tbl_name
  out <- list()
  out$res <- res
  out$date_end <- zoo::index(r)[nrow(r)]
  return(out)
}

#' @export
run_ppu_monthly <- function(bucket, as_of = NULL) {
  if (is.null(as_of)) {
    as_of <- floor_date(Sys.Date(), "months") - 1
  }
  ppu_tbl <- try_read(bucket, "meta-tables/ppu-monthly.parquet")
  r <- read_ret(ppu_tbl$DtcName, bucket)
  r <- cut_time(r, date_end = as_of)
  dt_info <- ret_date_info(r)
  if (min(dt_info$End) < as_of) {
    as_of <- min(dt_info$End)
  }
  tbl_name <- unique(ppu_tbl$Table)
  res <- list()
  for (i in 1:length(tbl_name)) {
    res[[i]] <- ppu_sect(ppu_tbl, r, as_of, tbl_name[i], eom = TRUE)
  }
  names(res) <- tbl_name
  out <- list()
  out$res <- res
  out$date_end <- zoo::index(r)[nrow(r)]
  return(out)
}

#' @export
ppu_sect <- function(ppu_tbl, r, as_of, tbl_name = NULL, eom = FALSE) {
  if (!is.null(tbl_name)) {
    ppu_x <- ppu_tbl[ppu_tbl$Table %in% tbl_name, ]
  } else {
    ppu_x <- ppu_tbl
  }
  x <- r[, ppu_x$DtcName]
  x <- clean_by_col(x)
  if (!is.na(x$miss)) {
    warning("some returns missing")
    ppu_x <- ppu_x[ppu_x$DtcName %in% colnames(x$ret), ]
  }
  x <- x$ret
  perf <- tbl_cal_perf(x, as_of, eom)
  perf$Name <- gsub(" Daily Est.", "", perf$Name)
  yrs <- xts_to_dataframe(change_freq(x, "years"))
  yrs <- yrs[order(yrs$Date, decreasing = TRUE), ]
  yrs <- t(yrs[2:4, -1])
  y <- seq(from = year(as_of) - 1, to = year(as_of) - 3)
  colnames(yrs) <- as.character(y)
  xdf <- data.frame(
    Name = perf$Name, Target = ppu_x$Target / 100
  )
  xdf <- cbind(xdf, perf[, 2:8], yrs)
  xdf <- cbind(xdf, ppu_x[, c("Format", "DisplayName")])
  colnames(xdf)[13] <- "Format"
  xdf$Format[is.na(xdf$Format)] <- ""
  xdf <- xdf[, c("DisplayName", colnames(xdf)[2:(ncol(xdf)-1)])]
  colnames(xdf)[1] <- "Name"
  rownames(xdf) <- NULL
  column_defs <- setNames(
    lapply(colnames(xdf[, -1]), function(col) {
      if (col %in% c("DTD", "1 Yr", "2024")) {
        colDef(
          style = function(value, index, name) {
            list(borderLeft = "1px solid #000")
          },
          format = colFormat(percent = TRUE, digits = 2)
        )
      } else {
        colDef(
          format = colFormat(percent = TRUE, digits = 2)
        )
      }
    }),
    colnames(xdf)[-1]
  )
  column_defs$Format <- colDef(show = FALSE)
  column_defs$Name <- colDef(width = 200, sticky = "left")
  rdf <- reactable(
    xdf,
    pagination = FALSE,
    sortable = FALSE,
    theme = reactableTheme(
      headerStyle = list(
        backgroundColor =  "#003057",
        color = "white",
        fontWeight = "bold"
      )
    ),
    columns = column_defs,
    rowStyle = function(index) {
      if (xdf[index, "Format"] == "bold") {
        list(`font-weight` = "bold")
      } else if (xdf[index, "Format"] == "bench") {
        list(background = "lightgray")
      } else if (xdf[index, "Format"] == "lowgrid") {
        list(borderBottom = "1px solid #000")
      } else if (xdf[index, "Format"] == "lowgrid-bench") {
        list(background = "lightgray", borderBottom = "1px solid #000")
      }
    }
  )
  res <- list()
  res$react <- rdf
  res$num_df <- xdf
  return(res)
}

#' @export
tbl_cal_perf <- function(x, as_of = NULL, eom = FALSE) {
  dt <- eom_cal_perf_dt(as_of, eom)
  perf <- list()
  for (i in 1:length(dt)) {
    perf[[i]] <- apply(x[paste0(dt[i], "/", as_of)] + 1, 2, prod)
    if (grepl("Yr", names(dt)[i])) {
      a <- as.numeric(gsub(" Yr", "", names(dt[i])))
      perf[[i]] <- perf[[i]]^(1/a) - 1
    } else {
      perf[[i]] <- perf[[i]] - 1
    }
  }
  perf <- do.call("cbind", perf)
  perf <- data.frame(
    Name = rownames(perf),
    perf,
    row.names = NULL
  )
  colnames(perf)[2:ncol(perf)] <- names(dt)
  return(perf)
}

#' @title Get Common Trailing Performance Dates
#' @param as_of starting date, default is end of last month
#' @param eom option to use end of month dates if working with monthly data,
#'   default is TRUE
#' @return vector of dates for common trailing periods, see details
#' @details
#'   Will return DTD, MTD, QTD, 1 Yr, 3 Yr, 5 Yr, and 10 Yr trailing periods.
#'   If \code{eom} parameter is set to TRUE then DTD will be omitted.
#' @export
eom_cal_perf_dt <- function(as_of = NULL, eom = TRUE) {
  if (is.null(as_of)) {
    as_of <- lubridate::floor_date(Sys.Date(), "months") - 1
  } else {
    as_of <- try(as.Date(as_of))
    if (inherits(as_of, "try-error")) {
      stop("as_of could not be converted to date")
    }
  }
  dt <- c(
    as_of,
    floor_date(as_of, "months"),
    floor_date(as_of, "quarters"),
    floor_date(as_of, "years"),
    as_of - years(1),
    as_of - years(3),
    as_of - years(5),
    as_of - years(10)
  )
  nm <- c("DTD", "MTD", "QTD", "YTD", "1 Yr", "3 Yr", "5 Yr", "10 Yr")
  if (eom) {
    dt <- eo_month(dt)
    dt[5:8] <- eo_month(add_with_rollback(dt[5:8], months(1)))
    names(dt) <- nm
    dt <- dt[-1]
    return(dt)
  } else {
    names(dt) <- nm
    dt[5:8] <- next_trading_day(dt[5:8], 1)
    return(dt)
  }
}

port_from_xl <- function(wb, sht, bucket, asset_freq = "D",
                         rebal_freq = "M", nm = "Port") {
  wgt <- read_xts(wb, sht)
  r <- read_ret(colnames(wgt), bucket)
  r[is.na(r)] <- 0
  p <- Rebal$new(wgt, r, asset_freq, rebal_freq, nm)
  p$rebal()
  return(p)
}

write_imb <- function(bucket, t_minus_m = 0) {

  pres <- read_pptx("N:/Investment Team/REPORTING/IMB/imb-writer/template.pptx")
  as_of <- floor_date(add_with_rollback(Sys.Date(), months(-t_minus_m)),
                      "months") - 1
  wb <- "N:/Investment Team/REPORTING/IMB/imb-writer/imb-data-input.xlsx"
  dict <- readxl::read_excel(wb, "data")
  descr <- readxl::read_excel(wb, "descr")

  loc <- set_loc()

  # core fixed income
  x <- read_ret(c("Core Fixed Income", "Bloomberg Barclays U.S. Aggregate",
                  "BofAML U.S. Treasury Bill 3M"), bucket)
  res <- clean_asset_bench_rf(x[, 1], x[, 2], x[, 3], date_end = as_of)
  pres <- imb_bond_slide(pres, res, dict, descr, loc$bond_ctf,
                         "Core Fixed Income", "Core Fixed Income CTF", as_of,
                         "Sector", alloc = TRUE)
}

#' @export
set_loc <- function() {
  bond <- list(
    descript = c(left = 0.34, top = 0.75),
    maturity = c(left = 2.46, top = 1.97, height = 1.31, width = 4.2),
    pie = c(left = 6.47, top = 0.95, height = 2.04, width = 2.76),
    stats = c(left = 0.34, top = 2.39),
    char = c(left = 0.34, top = 4.21),
    wealth = c(left = 2.56, top = 3.28, height = 2.84, width = 3.36),
    capm = c(left = 6.17, top = 3.28, height = 2.83, width = 3.36),
    perf = c(left = 0.34, top = 6.05),
    alloc = c(left = 0.34, top = 1.67, height = 1)
  )
  bond_ctf <- bond
  bond_ctf$descript["top"] <- 0.89
  bond_ctf$alloc["top"] <- 2.21
  bond_ctf$alloc["width"] <- 2.22
  bond_ctf$stats["top"] <- 3.05
  bond_ctf$char["top"] <- 4.71
  stock = list(
    descript = c(left = 0.45, top = 0.93),
    bar = c(left = 4.81, top = 1.10, width = 4.79, height = 1.95),
    stats = c(left = 0.45, top = 3.17),
    char = c(left = 0.45, top = 4.91),
    wealth = c(left = 2.2, top = 3.17, height = 2.78, width = 3.6),
    capm = c(left = 6, top = 3.17, height = 2.78, width = 3.6),
    perf = c(left = 0.45, top = 3.17),
    alloc = c(left = 0.45, top = 2.05, height = 1.15)
  )
  alt = list(
    descript = c(left = 0.45, top = 0.93),
    alloc = c(left = 4.81, top = 1.10, width = 4.79, height = 1.95),
    stats = c(left = 0.45, top = 3.17),
    char = c(left = 0.45, top = 4.91),
    wealth = c(left = 2.2, top = 3.17, height = 2.78, width = 3.6),
    capm = c(left = 6, top = 3.17, height = 2.78, width = 3.6),
    perf = c(left = 0.45, top = 3.17)
  )
  res <- list()
  res$bond <- bond
  res$stock <- stock
  res$alt <- alt
  res$bond_ctf <- bond_ctf
  return(res)
}

#' @title IMB Trailing Performance Table
#' @param res from clean_asset_bench_rf()
#' @param as_of as of date for performance
#' @return flextable with trailing performance
#' @export
imb_trail_perf_tbl <- function(res, as_of) {
  dt <- eom_cal_perf_dt(as_of)
  fund <- res$x
  bench <- res$b
  rf <- res$rf
  cr <- matrix(nrow = 2, ncol = length(dt))
  # calculate performance
  for (i in 1:length(dt)) {
    # when dt >= 4 period is longer than 1 year, need to annualize
    if (i >= 4) {
      n <- as.numeric(gsub(" Yr", "", names(dt)[i]))
    } else {
      n <- 1
    }
    cr[1, i] <- prod(fund[paste0(dt[i], "/")] + 1)^(1/n)-1
    cr[2, i] <- prod(bench[paste0(dt[i], "/")] + 1)^(1/n)-1
  }
  # for look backs without enough observations report NA
  n_obs <- nrow(fund)
  if (n_obs < 120) {
    cr[, 7] <- NA
  }
  if (n_obs < 60) {
    cr[, 6] <- NA
  }
  if (n_obs < 36) {
    cr[, 5] <- NA
  }
  if (n_obs < 12) {
    cr[, 4] <- NA
  }
  # transform to data.frame and add asset names as first column
  cr <- data.frame(FUND = c(colnames(fund), colnames(bench)), cr)
  colnames(cr)[-1] <- names(dt)
  # calculate performance by year
  yrs <- change_freq(xts_cbind(fund, bench), "years")
  yrs <- xts_to_dataframe(yrs)
  yrs <- yrs[order(yrs$Date, decreasing = TRUE), ]
  # truncate to last 7 years
  yrs <- yrs[1:7, ]
  # transpose to horizontal layout to column bind to performance table
  df_yrs <- t(yrs[, -1])
  colnames(df_yrs) <- year(yrs$Date)
  cr <- cbind(cr, df_yrs)
  # add total row of fund less bench perf
  tot_row <- data.frame(
    RETURNS = paste0("+ / - ", colnames(bench)),
    cr[1, -1] - cr[2, -1]
  )
  colnames(tot_row) <- colnames(cr)
  cr <- rbind(cr, tot_row)
  # format to percent
  cr[, -1] <- apply(cr[, -1], 2, scales::percent, accuracy = 0.1)
  # output flextable
  col <- dtc_col()
  flextable(cr) |>
    theme_alafoli() |>
    font(part = "body", fontname = "Source Sans Pro Light") |>
    font(part = "header", fontname = "La Gioconda TT") |>
    color(i = 1, color = col["navyblue"], part = "header") |>
    color(part = "body", color = "black") |>
    width(1.75, j = 1) |>
    width(0.525, j = 2:ncol(cr)) |>
    height(0.18, i = 1:nrow(cr)) |>
    vline(j = 8, border = fp_border(color = "grey")) |>
    hline(i = 2, border = fp_border(color = "grey"))
}

#' @export
imb_stats_tbl <- function(res, as_of) {
  tm10 <- as_of - years(10)
  # the stats in performance table are a subset of the perf_summary output and
  # alpha
  mpt <- tbl_mpt(res, "months")
  ix <- c("Alpha", "Beta", "Sharpe Ratio", "Tracking Error", "Up Capture",
          "Down Capture")
  perf_tbl <- mpt$xdf[mpt$xdf$Metric %in% ix, 1:2]
  perf_tbl$Metric <- factor(perf_tbl$Metric, levels = ix)
  perf_tbl <- perf_tbl[order(perf_tbl$Metric), ]
  colnames(perf_tbl) <- c("STATISTICS", "FUND")
  # output in flextable
  col <- dtc_col()
  flextable(perf_tbl) |>
    theme_alafoli() |>
    font(part = "body", fontname = "Source Sans Pro Light") |>
    font(part = "header", fontname = "La Gioconda TT") |>
    color(i = 1, color = col["navyblue"], part = "header") |>
    color(part = "body", color = "black") |>
    height(0.18, i = 1:nrow(perf_tbl)) |>
    width(j = 1, width = 1.2)
}

#' @export
imb_wealth_cht <- function(res) {
  col <- dtc_col()
  plot_col <- col[c("navyblue", "pine")]
  plot_col <- unname(plot_col)
  dat <- res$xb
  viz_wealth_index(dat) +
    scale_color_manual(values = plot_col) +
    xlab("") + labs(title = "CUMULATIVE PERFORMANCE") +
    guides(color = guide_legend(nrow = 2, keyheight = 5,
                                default.unit = "pt")) +
    theme(
      plot.title = element_text(
        family = "La Gioconda TT",
        color = col["navyblue"],
        size = 8
      ),
      legend.position = "bottom",
      text = element_text(
        size = 7,
        family = "Source Sans Pro Light",
        color = "grey40"
      ),
      legend.box.spacing = unit(-10, "pt"),
      panel.background = element_blank(),
      strip.background = element_blank()
    )
}

#' @export
imb_capm_cht <- function(res, as_of, adj_scale = TRUE, legend_loc = "right") {
  tm10 <- as_of - years(10)
  plot_ret <- res$xb[paste0(tm10, "/")]
  plot_ret <- na.omit(plot_ret)
  plot_y <- calc_geo_ret(plot_ret, "months")
  plot_x <- calc_vol(plot_ret, "months")
  plot_dat <- data.frame(
    x = plot_x,
    y = plot_y,
    name = colnames(plot_ret)
  )
  plot_dat$Bench <- plot_dat$name %in% colnames(res$b)
  plot_dat$shape <- ifelse(plot_dat$Bench, 17, 16)
  plot_dat$name <- factor(plot_dat$name, unique(plot_dat$name))
  col <- set_plot_col(nrow(plot_dat))
  col <- unname(col)
  res <- ggplot(plot_dat, aes(x = x, y = y, col = name)) +
    geom_point(size = 3, shape =plot_dat$shape) +
    geom_vline(xintercept = plot_dat$x[plot_dat$Bench], color = "grey") +
    geom_hline(yintercept = plot_dat$y[plot_dat$Bench], color = "grey") +
    scale_color_manual(values = col) +
    guides(shape = element_blank()) +
    labs(col = "", title = "RISK & RETURN") +
    xlab("Annualized Risk") + ylab("Annualized Return") +
    scale_x_continuous(labels = scales::percent) +
    scale_y_continuous(labels = scales::percent) +
    theme_minimal() +
    theme(
      panel.grid.major = element_blank(),
      panel.grid.minor = element_blank(),
      panel.background = element_blank(),
      text = element_text(
        size = 7,
        family = "Source Sans Pro Light",
        color = "grey40"
      ),
      plot.title = element_text(
        family = "La Gioconda TT",
        color = col[1],
        size = 8
      ),
      legend.position = legend_loc
    )
  if (adj_scale) {
    x_max <- max(plot_dat$x + 0.01, 0.04)
    x_min <- 0
    y_max <- max(plot_dat$y)
    if (y_max < 0) {
      y_min <- min(y_max - 0.01, -0.04)
      y_max <- -y_min
    } else {
      y_max <- y_max + 0.01
      y_min <- -0.02
    }
    res <- res +
      scale_x_continuous(limits = c(x_min, x_max), labels = scales::percent) +
      scale_y_continuous(limits = c(y_min, y_max), labels = scales::percent)
  }
  if (legend_loc == "bottom") {
    res <- res +
      guides(color = guide_legend(nrow = 2, keyheight = 5,
                                  default.unit = "pt"))
  }
  return(res)
}

#' @export
imb_alloc_tbl <- function(dict) {
  col <- dtc_col()
  wgt <- dict[dict$DataType == "Allocation", c("Field", "Value")]
  style <- dict[dict$DataType == "Style", c("Field", "Value")]
  if (nrow(style) > 0) {
    tbl <- left_join(wgt, style, by = "Field")
    colnames(tbl) <- c("ALLOCATION", "Target Weight", "Style")
    tbl <- tbl[, c(1, 3, 2)]
  } else {
    tbl <- wgt
    colnames(tbl) <- c("ALLOCATION", "Target Weight")
  }
  tbl$`Target Weight` <- scales::percent(as.numeric(tbl$`Target Weight`) / 100,
                                         0.1)
  flextable(tbl) |>
    theme_alafoli() |>
    font(part = "body", fontname = "Source Sans Pro Light") |>
    font(part = "header", fontname = "La Gioconda TT") |>
    color(i = 1, color = col["navyblue"], part = "header") |>
    color(part = "body", color = "black") |>
    height(0.025, i = 1:nrow(tbl)) |>
    line_spacing(space = 0.5) |>
    line_spacing(space = 0.75, i = 1) |>
    hrule(rule = "exact") |>
    width(j = 1, width = 1.75) |>
    width(j = 2:ncol(tbl), width = 1.25)
}

#' @export
imb_descr_tbl <- function(descr) {
  col <- dtc_col()
  tbl <- data.frame(DESCRIPTION = descr$Description)
  flextable(tbl) |>
    theme_alafoli() |>
    font(part = "body", fontname = "Source Sans Pro Light") |>
    font(part = "header", fontname = "La Gioconda TT") |>
    color(i = 1, color = col["navyblue"], part = "header") |>
    color(part = "body", color = "black") |>
    width(j = 1, width = 4.25) |>
    border_remove()
}



#' @export
imb_char_tbl <- function(xdf) {
  col <- dtc_col()
  per_fld <- xdf[[1]] %in% c("Dividend Yield", "Expense Ratio", "SEC Yield",
                             "YTM")
  num_fld <- xdf[[1]] %in% c("TTM P/E Ratio", "TTM P/B Ratio", "Duration")
  cur_fld <- xdf[[1]] %in% "AUM (MMs)"
  fdf <- xdf
  fdf[per_fld, 2] <- scales::percent(as.numeric(xdf[per_fld, 2]),
                                     accuracy = 0.1)
  fdf[num_fld, 2] <- scales::number(as.numeric(xdf[num_fld, 2]), accuracy = 0.1)
  fdf[cur_fld, 2] <- scales::number(as.numeric(xdf[num_fld, 2]), big.mark = ",")
  flextable(fdf) |>
    theme_alafoli() |>
    font(part = "body", fontname = "Source Sans Pro Light") |>
    font(part = "header", fontname = "La Gioconda TT") |>
    color(i = 1, color = col["navyblue"], part = "header") |>
    height(0.18, i = 1:nrow(fdf)) |>
    width(j = 1, width = 1.2)
}

#' @export
imb_maturity_cht <- function(xdf) {
  col <- set_plot_col(nrow(xdf))
  col <- unname(col)
  xdf$Value <- as.numeric(xdf$Value) / 100
  xdf$Lbl <- scales::percent(xdf$Value, 0.1)
  xdf$Field <- factor(xdf$Field, unique(xdf$Field))
  ggplot(xdf, aes(x = Field, y = Value, label = Lbl, fill = Field)) +
    geom_bar(stat = "identity", position = "dodge") +
    scale_fill_manual(values = col) +
    geom_text(
      aes(y = Value + 0.025),
      size = 1.75,
      position = position_dodge(0.9),
      color = "grey40") +
    xlab("") + ylab("") + labs(title = "MATURITY", fill = "") +
    theme_minimal() +
    theme(
      panel.grid.major = element_blank(),
      panel.grid.minor = element_blank(),
      panel.background = element_blank(),
      text = element_text(
        size = 6,
        family = "Source Sans Pro Light",
        color = "grey40"
      ),
      plot.title = element_text(
        family = "La Gioconda TT",
        color = col[1],
        size = 8
      ),
      legend.position = "none",
      axis.text.y = element_blank(),
      axis.text.x = element_text(
        size = 5,
        family = "Source Sans Pro Light",
        color = "grey40"
      )
    )
}

#' @export
imb_pie_cht <- function(xdf, pie_type) {
  xdf$Value <- as.numeric(xdf$Value)
  xdf$Label <- scales::percent(xdf$Value / 100, 0.1)
  col <- set_plot_col(nrow(xdf))
  col <- unname(col)
  ggplot(xdf, aes(x = "", y = Value, fill = Field, label = Label)) +
    geom_bar(stat = "identity", width = 1) +
    coord_polar("y", start = 0) +
    theme_void() +
    scale_fill_manual(values = col) +
    labs(fill = "", title = toupper(pie_type)) +
    geom_text(position = position_stack(0.5), col = "white", size = 2) +
    theme(
      plot.title = element_text(
        family = "La Gioconda TT",
        color = col[1],
        size = 8
      ),
      legend.text = element_text(
        size = 6,
        family = "Source Sans Pro Light",
        color = "grey40"
      ),
      legend.key.size = unit(0.2, "in"),
      legend.box.spacing = unit(-10, "pt")
    )
}

imb_bond_slide <- function(pres, res, dict, descr, loc, dtc_name, slide_title,
                           as_of, pie_type = c("Quality", "Sector"),
                           asset_res = NULL, alloc = FALSE) {
  pie_type <- pie_type[1]
  set_flextable_defaults(font.size = 8, font.color = "black")
  dict <- dict[dict$Page == dtc_name, ]
  if (nrow(dict) == 0) {
    stop(paste0(dtc_name, " not found in dictionary"))
  }
  descr <- descr[descr$Page == dtc_name, ]
  if (nrow(dict) == 0) {
    stop(paste0(dtc_name, " not found in description"))
  }
  tbl_descr <- imb_descr_tbl(descr)
  cht_maturity <- imb_maturity_cht(filter(dict, DataType %in% "Maturity"))
  cht_pie <- imb_pie_cht(filter(dict, DataType %in% pie_type), pie_type)
  tbl_stats <- imb_stats_tbl(res, as_of)
  xdf <- filter(dict, DataType %in% "Characteristics") |>
    as.data.frame()
  tbl_char <- imb_char_tbl(xdf[, 3:4])
  cht_wealth <- imb_wealth_cht(res)
  cht_capm <- imb_capm_cht(res, as_of, legend_loc = "bottom")
  tbl_trail_perf <- imb_trail_perf_tbl(res, as_of) |>
    width(2.25, j = 1)
  pres <- add_slide(pres, layout = "Body Slide", master = "DTC-Theme-202109") |>
    ph_with(slide_title, ph_location_label("Text Placeholder 18")) |>
    # description
    ph_with(
      tbl_descr,
      ph_location(
        left = loc$descript["left"],
        top = loc$descript["top"])) |>
    # maturity
    ph_with(
      cht_maturity,
      ph_location(
        left = loc$maturity["left"],
        top = loc$maturity["top"],
        width = loc$maturity["width"],
        height = loc$maturity["height"])) |>
    # pie
    ph_with(
      cht_pie,
      ph_location(
        left = loc$pie["left"],
        top = loc$pie["top"],
        height = loc$pie["height"],
        width = loc$pie["width"])) |>
    # stats
    ph_with(
      tbl_stats,
      ph_location(
        left = loc$stats["left"],
        top = loc$stats["top"])) |>
    # characteristics
    ph_with(
      tbl_char,
      ph_location(
        left = loc$char["left"],
        top = loc$char["top"])) |>
    # wealth
    ph_with(
      cht_wealth,
      ph_location(
        left = loc$wealth["left"],
        top = loc$wealth["top"],
        height = loc$wealth["height"],
        width = loc$wealth["width"])) |>
    # capm
    ph_with(
      cht_capm,
      ph_location(
        left = loc$capm["left"],
        top = loc$capm["top"],
        height = loc$capm["height"],
        width = loc$capm["width"])) |>
    # perf
    ph_with(
      tbl_trail_perf,
      ph_location(
        left = loc$perf["left"],
        top = loc$perf["top"])) |>
    # as of date
    ph_with(
      format(as_of, "%B %Y"),
      ph_location_label("Text Placeholder 3")
    )

  if (alloc) {
    tbl_alloc <- imb_alloc_tbl(dict)
    pres <- ph_with(
      pres,
      tbl_alloc,
      ph_location(
        left = loc$alloc["left"],
        top = loc$alloc["top"],
        height = loc$alloc["height"],
        width = loc$alloc["width"]
      ))
  }
  return(pres)
}
