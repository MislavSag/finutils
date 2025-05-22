#' Process and Clean Quantconnect Hour Price Data
#'
#' This function processes raw hour price data, adjusts for splits/dividends, calculates
#' returns, and removes duplicates and invalid data points.
#'
#' @param file_path Character. Path to the CSV file containing the price data.
#' @param symbols Character. Symbols to include in the analysis. Default is NULL, which means all symbols are included.
#' @param first_date Date. The first date to include in the analysis. Default is NULL, which means all dates are included.
#' @param min_obs Integer. Minimum number of observations required per symbol. Default is 253.
#' @param duplicates Character. Method for handling duplicate symbols. Options are "slow", "fast", or "none". Default is "slow".
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
                   duplicates = c("slow", "fast", "none"),
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
  # symbols = c("spy", "aapl", "msft", "a", "efav")
  # first_date = Sys.Date() - 100
  # first_date = NULL

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
  assert_date(first_date, any.missing = FALSE, null.ok = TRUE)

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
  prices[, date := with_tz(date, tzone = "UTC")]
  prices[, date := force_tz(date, tzone = "America/New_York")]

  # Set keys
  setkey(prices, "symbol")
  setorder(prices, symbol, date)

  # Remove duplicates
  prices = unique(prices, by = c("symbol", "date"))

  # Handle duplicate symbols (e.g., PHUN and PHUN.1)
  duplicates = match.arg(duplicates)
  if (duplicates == "fast") {
    dups = prices[, .(symbol, n = .N),
                  by = .(date, open, high, low, close, volume, adj_close, symbol_first = substr(symbol, 1, 1))]
    dups = dups[n > 1]
    dups[, symbol_short := gsub("\\.\\d$", "", symbol)]
    symbols_remove = dups[, .(symbol, n = .N),
                          by = .(date, open, high, low, close, volume, adj_close, symbol_short)]
    symbols_remove = symbols_remove[n >= 2, unique(symbol)]
    symbols_remove = symbols_remove[grepl("\\.", symbols_remove)]
  } else if (duplicates == "slow") {
    symbols_remove = c(
      "acet.2", "adv.2", "altg.1", "anix.1", "arct.2", "asb.3", "atcx.1",
      "bbcp.1", "beem.1", "bfi.2", "bki.2", "btdr.1", "btop.1", "capr.2",
      "carr.1", "cbre.1", "cenn.1", "cgnt.1", "corz.1", "crdf.1", "cwen.a",
      "cwen.a.1", "dbrg.1", "dkng.1", "drts.1", "eftr.1", "emdd.1", "eose.1",
      "eqos.1", "escr.2", "eseb.1", "eshy.1", "esi.2", "eurz.2", "fbrx.1",
      "fg.2", "flnt.1", "fnjn.1", "free.3", "frg.4", "fslf.1", "fstx.1",
      "gci.1", "gla.2", "glcn.1", "glin.1", "goev.1", "gpt.1", "gpt.2",
      "grmy.1", "gsd.2", "hffg.1", "hgen.1", "hsto.1", "hymc.1", "id.2",
      "iova.1", "iqv.1", "ives.1", "j.1", "jnmf.1", "kcg.1", "lazr.2",
      "lcii.1", "lmb.1", "lpro.1", "lrmr.1", "lsi.2", "lstza.1", "lstzb.1",
      "math.2", "mdgl.1", "msgs.1", "nes.2", "nfh.1", "nmtr.1", "norw.1",
      "on.1", "otis.1", "otlk.1", "phge.1", "phun.1", "pipr.1", "plcy.1",
      "pme.3", "poly.2", "prg.4", "prmw.1", "pstv.2", "qlgn.1", "rbc.1",
      "rcm.2", "rdog.1", "ren.1", "resp.2", "ride.2", "rosc.1", "rvph.1",
      "ryce.1", "scu.3", "shyf.1", "siox.1", "sj.1", "spcb.1", "spgm.1",
      "spru.1", "swbi.2", "tbio.3", "teum.1", "thtx.1", "timb.1", "tpco.1",
      "tvtx.1", "usrt.1", "utz.1", "vate.1", "via.1", "vldr.1", "vln.1",
      "vnt.2", "vrt.1", "wrap.1", "wtre.1", "xela.1", "xrdc.1", "xtnt.2")
  } else {
    symbols_remove = NULL
  }
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
