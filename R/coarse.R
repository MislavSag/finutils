#' Coarse filtering
#'
#' Coarse equity filtering.
#'
#' @param min_mean_mon_price Minimum mean monthly price (default 1)
#' @param min_mean_mon_volume Minimum mean monthly volume (default 10000)
#' @param min_last_mon_mcap Minimum last month market cap (default 1e9)
#' @param dollar_vol_n If not NULL, select top n by dollar volume each month
#' @param ... Arguments passed to \code{qc_daily_parquet}
#'
#' @return Data frame of daily prices
#' @import data.table
#' @importFrom stats na.omit
#' @importFrom data.table .N
#' @import checkmate
#' @export
coarse = function(min_mean_mon_price  = 1,
                  min_mean_mon_volume = 10000,
                  min_last_mon_mcap   = 1e9,
                  dollar_vol_n = NULL,
                  ...) {
  # Debug
  # library(finutils)
  # library(data.table)
  # prices = qc_daily_parquet(
  #     file_path = "F:/lean/data/all_stocks_daily",
  #     min_obs = 2 * 252,
  #     price_threshold = 1e-008,
  #     duplicates = "fast",
  #     add_dv_rank = FALSE,
  #     add_day_of_month = FALSE,
  #     etfs = FALSE
  #     # market_cap_fmp_file =
  #     )

  m = close_raw = symbol = universe_avg_price = avg_price = volume =
    universe_avg_vol = avg_volume = `.` = universe = dollar_vol =
    dollar_vol_rank = dollar_vol_sum = NULL

  # Import daily data
  prices = qc_daily_parquet(...)

  # Crate time variables
  prices[, m := data.table::yearmon(date)]

  # Averge unadjusted price > min_mean_mon_price
  coarse_avg_price = prices[, .(avg_price = mean(close_raw, na.rm = TRUE)), by = .(symbol, m)]
  coarse_avg_price[, universe_avg_price := FALSE]
  coarse_avg_price[shift(avg_price) >= min_mean_mon_price, universe_avg_price := TRUE, by = symbol]
  sprintf("we removed %f percent of rows",
          round(sum(coarse_avg_price$universe_avg_price == FALSE, na.rm = TRUE) / nrow(coarse_avg_price) * 100, 2))

  # Average volume > min_mean_mon_volume
  coarse_avg_vol = prices[, .(avg_volume = mean(volume)), by = .(symbol, m)]
  coarse_avg_vol[, universe_avg_vol := FALSE]
  coarse_avg_vol[shift(avg_volume) >= min_mean_mon_volume, universe_avg_vol := TRUE, by = symbol]
  sprintf("we removed %f percent of rows",
          round(sum(coarse_avg_vol$universe_avg_vol == FALSE) / nrow(coarse_avg_vol) * 100, 2))

  # # Average market cap > 1 bil
  # coarse_avg_mcap = prices[, .(avg_mcap = data.table::last(marketCap)), by = .(symbol, q)]
  # coarse_avg_mcap[, universe_avg_mcap := FALSE]
  # coarse_avg_mcap[shift(avg_mcap) >= 1e9, universe_avg_mcap := TRUE, by = symbol]
  # print(paste0("We remove ",
  #              round(sum(coarse_avg_mcap$universe_avg_mcap == FALSE) / nrow(coarse_avg_mcap) * 100),
  #              "% of rows."))

  # Combine coarse
  coarse = merge(coarse_avg_price, coarse_avg_vol, by = c("symbol", "m"))
  # coarse = merge(coarse, coarse_avg_mcap, by = c("symbol", "m"))
  coarse[, universe := (universe_avg_vol + universe_avg_price) == 2]
  coarse = coarse[m > 1998.1]

  # Merge prices and coarse
  prices = merge(prices, coarse[, .(symbol, m, universe)], by = c("symbol", "m"), all.x = TRUE)

  # select by dollar volume
  # dollar_vol_n = 1000
  if (!is.null(dollar_vol_n)) {
    prices = prices[universe == TRUE]
    prices[, dollar_vol := close_raw * volume]
    coarse_dollar_vol = prices[, .(dollar_vol_sum = sum(dollar_vol, na.rm = TRUE)), by = .(symbol, m)]
    coarse_dollar_vol[, dollar_vol_rank := frank(-dollar_vol_sum), by = m]
    prices = merge(prices, coarse_dollar_vol[, .(symbol, m, dollar_vol_rank)], by = c("symbol", "m"), all.x = TRUE)
    prices = prices[dollar_vol_rank <= dollar_vol_n]
  } else {
    prices = prices[universe == TRUE]
  }
  prices[, c("m", "universe") := NULL]
  return(prices)
}
