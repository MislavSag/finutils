#' Process and Clean Quantconnect Hour Price Data
#'
#' This function processes raw hour price data, adjusts for splits/dividends, calculates
#' returns, and removes duplicates and invalid data points.
#'
#' @param file_path Character. Path to the CSV file containing the price data.
#' @param symbols Character. Symbols to include in the analysis. Default is NULL, which means all symbols are included.
#' @param first_date Date. The first date to include in the analysis. Default is NULL, which means all dates are included.
#' @param min_obs Integer. Minimum number of observations required per symbol. Default is 253.
#' @param price_threshold Numeric. Minimum allowed price for open, high, low, and close columns. Default is 1e-8.
#' @param market_symbol Character. Symbol representing the market index (e.g., "spy").
#'  Default is NULL, which means you don't want to use add market data.
#' @param add_dv_rank Logical. Whether to add a rank by dollar volume for every date. Default is TRUE.
#'
#' @return A cleaned and processed data.table with price and return information.
#' @import data.table
#' @importFrom stats na.omit
#' @importFrom data.table .N
#' @import checkmate
#' @importFrom dplyr select filter collect rename_with all_of
#' @importFrom arrow open_dataset
#' @importFrom xts as.xts
#' @importFrom lubridate force_tz
#' @export
qc_hour = function(file_path,
                   symbols = NULL,
                   first_date = NULL,
                   min_obs = 253,
                   price_threshold = 1e-8,
                   market_symbol = NULL,
                   add_dv_rank = TRUE) {
  # Debug
  # library(data.table)
  # library(lubridate)
  # library(checkmate)
  # library(arrow)
  # library(dplyr)
  # file_path = "F:/lean/data/stocks_hour.csv"
  # symbols = c("spy", "aapl", "msft")
  # first_date = Sys.Date() - 100

  symbol = high = low = volume = adj_close = n = symbol_short = adj_rate =
    returns = N = `.` = dollar_vol_rank = close_raw = day_of_month = date_ = NULL

  # Validate inputs using checkmate
  assert_file_exists(file_path, access = "r")
  assert_integerish(min_obs,
                    lower = 1,
                    len = 1,
                    any.missing = FALSE)
  assert_numeric(
    price_threshold,
    lower = 0,
    len = 1,
    any.missing = FALSE
  )
  assert_logical(add_dv_rank, len = 1, any.missing = FALSE)
  assert_date(first_date, any.missing = FALSE)

  # Import data using arrow
  prices = open_dataset(file_path, format = "csv") |>
    rename_with(~ gsub(" ", "_", tolower(.x)))
  if (!is.null(symbols)) {
    prices = prices |>
      filter(symbol %in% symbols)
  }
  if (!is.null(first_date)) {
    prices = prices |>
      filter(date >= first_date)
  }
  prices = collect(prices)
  setDT(prices)

  # Convert timezone
  prices[, date := force_tz(date, tzone = "America/New_York")]

  # Set keys
  setkey(prices, "symbol")
  setorder(prices, symbol, date)

  # Remove duplicates
  prices = unique(prices, by = c("symbol", "date"))

  # Handle duplicate symbols (e.g., PHUN and PHUN.1)
  dups = prices[, .(
    open = data.table::first(open),
    high = max(high),
    low = min(low),
    close = data.table::last(close),
    adj_close = data.table::last(adj_close),
    volume = sum(volume)
  ), by = .(symbol, date = as.Date(date))] |>
    _[, .(symbol, n = .N), by = .(date,
                                  open,
                                  high,
                                  low,
                                  close,
                                  volume,
                                  adj_close,
                                  symbol_first = substr(symbol, 1, 1))] |>
    _[n > 1]
  dups[, symbol_short := gsub("\\.\\d$", "", symbol)]
  symbols_remove = dups[, .(symbol, n = .N), by = .(date, open, high, low, close, volume, adj_close, symbol_short)]
  symbols_remove = symbols_remove[n >= 2, unique(symbol)]
  symbols_remove = symbols_remove[grepl("\\.", symbols_remove)]
  if (length(symbols_remove) > 0) {
    prices = prices[!.(symbols_remove)]
  }

  # Adjust prices for splits/dividends
  prices[, adj_rate := adj_close / close]
  prices[, `:=`(
    open = open * adj_rate,
    high = high * adj_rate,
    low = low * adj_rate
  )]
  setnames(prices, "close", "close_raw")
  setnames(prices, "adj_close", "close")
  prices[, adj_rate := NULL]
  setcolorder(prices,
              c("symbol", "date", "open", "high", "low", "close", "volume"))

  # Optionally remove rows with low prices
  if (!is.null(price_threshold)) {
    prices = prices[open > price_threshold & high > price_threshold &
                      low > price_threshold &
                      close > price_threshold]
  }

  # Calculate returns
  prices[, returns := close / shift(close, 1) - 1, by = symbol]

  # Remove missing values
  prices = na.omit(prices)

  # Set market returns
  if (!is.null(market_symbol)) {
    spy_ret = na.omit(prices[symbol == market_symbol, .(date, market_ret = returns)])
    prices = spy_ret[prices, on = "date"]
    setkey(prices, symbol)
    setorder(prices, symbol, date)
  }

  # Remove symbols with insufficient data
  remove_symbols = prices[, .N, by = symbol][N < min_obs, symbol]
  if (length(remove_symbols) > 0) {
    prices = prices[!.(remove_symbols)]
  }

  # Create rank by volume for every date
  if (add_dv_rank == TRUE) {
    prices[, dollar_vol_rank := frankv(close_raw * volume, order = -1L), by = as.Date(date)]
  }

  # Add day of month column
  # if (add_day_of_month == TRUE) {
  #   setorder(prices, symbol, date)
  #   prices[, month := yearmon(date)]
  #   month_date = unique(prices[, .(month, date)])
  #   setorder(month_date, date)
  #   month_date[, day_of_month := 1:.N, by = .(month)]
  #   prices = month_date[, .(date, day_of_month)][prices, on = c("date")]
  #   prices[, day_of_month := as.factor(day_of_month)]
  #   prices[day_of_month == 23, day_of_month := 22] # 23 day to 22 day
  #   prices[day_of_month == 22, day_of_month := 21] # not sure about this but lets fo with it
  # }

  return(prices)
}
