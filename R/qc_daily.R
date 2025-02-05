#' Process and Clean Quantconnect Price Data
#'
#' This function processes raw price data, adjusts for splits/dividends, calculates
#' returns, and removes duplicates and invalid data points.
#'
#' @param file_path Character. Path to the CSV file containing the price data.
#' @param symbols Character. Symbols to include in the analysis. Default is NULL, which means all symbols are included.
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
#' @export
qc_daily = function(file_path,
                    symbols = NULL,
                    min_obs = 253,
                    price_threshold = 1e-8,
                    market_symbol = NULL,
                    add_dv_rank = TRUE) {

  # Debug
  # file_path = "F:/lean/data/stocks_daily.csv"
  # symbols = c("spy", "aapl", "msft")

  symbol = high = low = volume = adj_close = n = symbol_short = adj_rate =
    returns = N = `.` = dollar_vol_rank = close_raw = NULL

  # Validate inputs using checkmate
  assert_file_exists(file_path, access = "r")
  assert_integerish(min_obs, lower = 1, len = 1, any.missing = FALSE)
  assert_numeric(price_threshold, lower = 0, len = 1, any.missing = FALSE)

  # Load data
  prices = fread(file_path)
  setnames(prices, gsub(" ", "_", tolower(colnames(prices))))

  # Set keys
  setkey(prices, "symbol")
  setorder(prices, symbol, date)

  # Filter symbols
  if (!is.null(symbols)) {
    prices = prices[.(symbols), nomatch = NULL]
    setkey(prices, "symbol")
    setorder(prices, symbol, date)
  }

  # Remove duplicates
  prices = unique(prices, by = c("symbol", "date"))

  # Handle duplicate symbols (e.g., PHUN and PHUN.1)
  dups = prices[, .(symbol, n = .N),
                by = .(date, open, high, low, close, volume, adj_close, symbol_first = substr(symbol, 1, 1))]
  dups = dups[n > 1]
  dups[, symbol_short := gsub("\\.\\d$", "", symbol)]
  symbols_remove = dups[, .(symbol, n = .N),
                        by = .(date, open, high, low, close, volume, adj_close, symbol_short)]
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
  setcolorder(prices, c("symbol", "date", "open", "high", "low", "close", "volume"))

  # Optionally remove rows with low prices
  if (!is.null(price_threshold)) {
    prices = prices[open > price_threshold & high > price_threshold &
                      low > price_threshold & close > price_threshold]
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
    prices[, dollar_vol_rank := frankv(close_raw * volume, order = -1L), by = date]
  }

  return(prices)
}
