#' Import crypto data
#'
#' This function processes hour crypto makret data and marcet cap data
#'
#' @param path_market_cap Character. Path to market cap data.
#' @param path_prices Character. Path to prices data.
#' @param first_date Date. First date to look at.
#' @param snapshot_hour Numeric. Hours to keep from hour data.
#' @param n Integer Number of coins to keep from market cap
#' @param min_constituents Integer. Minimum number of constituents.
#'
#' @return A cleaned and processed data.table with prices and universe data.
#' @import data.table
#' @importFrom stats na.omit
#' @importFrom data.table .N
#' @import checkmate
#' @importFrom arrow read_feather
#' @importFrom xts as.xts
#' @importFrom lubridate force_tz
#' @export
crypto = function(
  path_market_cap,
  path_prices,
  first_date = as.Date("2018-08-05"),
  snapshot_hour = 0,
  n = 10,
  min_constituents = 10) {

  path_nmarket_cap = Date = Ticker = volume = MarketCapUSD = Datetime = 
    Open = High = Low = Close = Volume = Hour = cap_rank = is_index =
    log_returns = ewvol = N = log_return = `.` = NULL
  
  # Debug
  # path_market_cap = "H:/strategies/cryptotemp/coincodex_marketcap.feather"
  # path_prices     = "H:/strategies/cryptotemp/binance_spot_1h.feather"
  # snapshot_hour = 0 # 0, 12 for example
  # n = 10
  # min_constituents = 10

  # Market Capitalization
  market_cap = read_feather(path_market_cap)
  setDT(market_cap)
  market_cap = market_cap[Date >= first_date]
  market_cap = market_cap[, .(Ticker, Date, MarketCapUSD)]

  # Prices
  prices = read_feather(path_prices)
  setDT(prices)
  prices = prices[Datetime >= as.POSIXct(first_date, tz="UTC")]
  prices = prices[, .(Ticker, Datetime, Open, High, Low, Close, Volume)]
  prices[, Datetime := force_tz(Datetime, tzone = "UTC")]

  # Create universe
  daily_prices = prices[, .(Ticker, Datetime, Open, High, Low, Close, Volume)]
  daily_prices[, Ticker := gsub("USDT$", "", Ticker)]
  daily_prices[, Hour := data.table::hour(Datetime)]
  daily_prices[, Date := as.Date(Datetime, tz = "UTC")]
  daily_prices = daily_prices[Hour == snapshot_hour]
  daily_prices = daily_prices[, .(Date, Ticker, Open, High, Low, Close, Volume)]

  # list of stablecoins from defi llama
  url = "https://stablecoins.llama.fi/stablecoins?includePrices=true"
  response = httr::GET(url)
  stables = httr::content(response)
  stables = sapply(stables$peggedAssets, `[[`, "symbol")

  # duplicates and others to remove
  to_remove = c(
    'BTCD', 'HBTC', 'IBBTC', 'RBTC', 'SBTC3', 'WNXM', 'WBTC', 'BNBBULL', 'BNBBEAR', 
    'EOSBULL', 'EOSBEAR', 'ETHBULL', 'ETHBEAR', 'XRPBULL', 'XRPBEAR')

  # Remove stables and manually removed
  dt = daily_prices[Ticker %notin% c(stables, to_remove)]

  # remove from market_cap data anything that wasn't trading on Binance or is a stable/duplicate
  binance_tickers = dt[, unique(Ticker)]
  mcap = mcap[Ticker %notin% c(stables, to_remove)] |>
    _[Ticker %in% binance_tickers]

  # get first date where we have min_constituents
  start_date = dt[, .N, by = Date] |>
    _[N >= min_constituents] |>
    _[, min(Date)]
  start_date = start_date + 1

  # Flag universe consitutents
  # For a given ticker/date observation, set is_index to TRUE if:
  #    - the asset was in the top 10 coins by market cap the day before
  #    - the date is on or after the date when we have min_consituents assets in our dataset
  universe = mcap[dt, on = .(Ticker, Date)]
  setorder(universe, Ticker, Date)
  universe[, MarketCapUSD := nafill(MarketCapUSD, type = "locf"), by = .(Ticker)]
  universe = na.omit(universe)
  universe[, cap_rank := frank(-MarketCapUSD, ties.method = "first"), by = Date]
  setorder(universe, Ticker, Date)
  universe[, is_index := shift(cap_rank) <= n & Date >= start_date, by = Ticker]
  universe = na.omit(universe)

  # function for calculating the ewma of a vector
  ewma = function(x, lambda) {
    ewma = vector(mode = "double", length = length(x))
    ewma[1] = x[1]
    for(i in 2:length(x)) {
      ewma[i]  = (1 - lambda)*x[i] + lambda*ewma[i-1]
    }
    ewma
  }

  # Caclulate EWMA vol
  lambda = 0.94
  setorder(universe, Ticker, Date)
  universe[, log_return := log(Close / shift(Close)), by = Ticker]
  universe = na.omit(universe)
  universe[, ewvol := sqrt(365)*sqrt(ewma(log_return**2,lambda = lambda)), by = Ticker]

  return(list(prices = prices, universe = universe))
}

