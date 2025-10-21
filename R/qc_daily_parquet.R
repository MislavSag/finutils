#' Process and Clean Quantconnect Parquet Price Data
#'
#' This function processes raw price data, adjusts for splits/dividends, calculates
#' returns, and removes duplicates and invalid data points.
#'
#' @param file_path Character. Path to the CSV file containing the price data.
#' @param market_cap_fmp_file Character. Path to the FMP cloud market cap file in Parquet format.
#' @param etfs Character. Can be TRUE (only ETF's), FALSE (only non ETF's),
#'     character (ETF constituents we want to follow) or NULL (all equities).
#'     IMPORTANT. This is relevant only from date 2009-08.
#' @param etf_cons Character vector of paths to QC etf universes. ETF constituents dummy variable.
#' @param symbols Character. Symbols to include in the analysis. Default is NULL, which means all symbols are included.
#' @param min_obs Integer. Minimum number of observations required per symbol. Default is 253.
#' @param duplicates Character. Method for handling duplicate symbols. Options are "slow", "fast", or "none". Default is "slow".
#' @param price_threshold Numeric. Minimum allowed price for open, high, low, and close columns. Default is 1e-8.
#' @param market_symbol Character. Symbol representing the market index (e.g., "spy").
#'  Default is NULL, which means you don't want to use add market data.
#' @param add_dv_rank Logical. Whether to add a rank by dollar volume for every date. Default is TRUE.
#' @param add_day_of_month Logical. Whether to add a day of month column. Default is FALSE.
#' @param profiles_fmp Logical. Whether to add profiles data from FMP cloud. Default is FALSE.
#' @param fmp_api_key Character. API key for FMP cloud. Required if `profiles_fmp` is TRUE.
#'
#' @return A cleaned and processed data.table with price and return information.
#' @import data.table
#' @importFrom stats na.omit
#' @importFrom data.table .N
#' @import checkmate
#' @importFrom xts as.xts
#' @import httr
#' @import Rcpp
#' @export
qc_daily_parquet = function(file_path,
                    market_cap_fmp_file = NULL,
                    etfs = NULL,
                    etf_cons = NULL,
                    profiles_fmp = FALSE,
                    symbols = NULL,
                    min_obs = 253,
                    duplicates = c("slow", "fast", "none"),
                    price_threshold = 1e-8,
                    market_symbol = NULL,
                    add_dv_rank = TRUE,
                    add_day_of_month = FALSE,
                    fmp_api_key = NULL
) {

  # Debug
  # library(data.table)
  # library(arrow)
  # library(httr)
  # library(dplyr)
  # library(checkmate)
  # file_path = "C:/Users/Mislav/qc_snp/data/all_stocks_daily"
  # market_cap_fmp_file = "C:/Users/Mislav/qc_snp/data/equity/us/fundamentals/market_cap.parquet"
  # symbols = c("amzn", "aapl", "msft", "tlt")
  # duplicates = "fast"
  # market_symbol = "spy"
  # etfs = FALSE
  # etf_cons = c("C:/Users/Mislav/qc_snp/data/equity/usa/universes/etf/iwm","C:/Users/Mislav/qc_snp/data/equity/usa/universes/etf/spy")
  # min_obs = 252
  # add_dv_rank = FALSE
  # add_day_of_month = FALSE
  # market_symbol = "spy"
  # profiles_fmp = TRUE
  # fmp_api_key = Sys.getenv("APIKEY")

  symbol = high = low = volume = adj_close = n = symbol_short = adj_rate =
    returns = N = `.` = dollar_vol_rank = close_raw = day_of_month =
    currency = country = isin = exchange = industry = sector = ipoDate = isEtf =
    isFund = fmp_symbol = qc_etf = etf = keep = threshold = symbol_join = NULL

  # Validate inputs using checkmate
  assert_directory_exists(file_path, access = "r")
  assert_integerish(min_obs, lower = 1, len = 1, any.missing = FALSE)
  assert_numeric(price_threshold, lower = 0, len = 1, any.missing = FALSE)
  assert_character(fmp_api_key, len = 1, any.missing = FALSE, null.ok = TRUE)
  assert_logical(add_dv_rank, len = 1, any.missing = FALSE)
  # if (is.null(etf_qc_path)) assert_directory_exists(etf_qc_path)

  # Import data using arrow
  prices = open_dataset(file_path) |>
    rename_with(~ gsub(" ", "_", tolower(.x)))
  if (!is.null(symbols)) {
    prices = prices |>
      filter(symbol %in% symbols)
  }
  if (!is.null(etfs)) {
    if (etfs == FALSE) {
      prices = prices |>
        filter(inv_vehicle == FALSE)
    } else if (etfs == TRUE ) {
      prices = prices |>
        filter(inv_vehicle == TRUE)
    }
  }
  prices = collect(prices)
  setDT(prices)

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
  setcolorder(prices, c("symbol", "date", "open", "high", "low", "close", "volume"))

  # Optionally remove rows with low prices
  if (!is.null(price_threshold)) {
    prices = prices[open > price_threshold & high > price_threshold &
                      low > price_threshold & close > price_threshold]
  }

  # Calculate returns
  prices[, returns := close / shift(close, 1) - 1, by = symbol]

  # Remove missing values
  prices = na.omit(prices, cols = c("symbol", "date", "close", "returns"))

  # Set market returns
  if (!is.null(market_symbol)) {
    market_ret = open_dataset(file_path) |>
      rename_with(~ gsub(" ", "_", tolower(.x))) |>
      filter(symbol == market_symbol) |>
      select(date, adj_close) |>
      collect()
    setDT(market_ret)
    market_ret[, paste0(market_symbol, "_returns") := adj_close / shift(adj_close) - 1]
    market_ret[, adj_close := NULL]
    prices = market_ret[prices, on = "date"]
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

  # Add day of month column
  if (add_day_of_month == TRUE) {
    setorder(prices, symbol, date)
    prices[, month := yearmon(date)]
    month_date = unique(prices[, .(month, date)])
    setorder(month_date, date)
    month_date[, day_of_month := 1:.N, by = .(month)]
    prices = month_date[, .(date, day_of_month)][prices, on = c("date")]
    prices[, day_of_month := as.factor(day_of_month)]
    prices[day_of_month == 23, day_of_month := 22] # 23 day to 22 day
    prices[day_of_month == 22, day_of_month := 21] # not sure about this but lets fo with it
  }

  # Add profiles data from FMP cloud
  if (profiles_fmp == TRUE) {
    print("This is slow. alternative in the future should be to use profile data from server")
    get_company_profile_bulk_all = function(start_part = 0L, max_parts = 4L, sleep_sec = 102) {
      url = "https://financialmodelingprep.com/stable/profile-bulk"
      res_list = vector("list", max_parts)
      i = start_part
      k = 1L

      repeat {
        if (i - start_part + 1L > max_parts) break
        resp = RETRY(
          "GET", url,
          query = list(part = i, apikey = fmp_api_key),
          httr::accept("text/csv"),
          times = 2L,
          pause_base = 201,
          pauce_cap = 301,
          pause_min = 201
        )
        cat("Step", i, "\n")
        print(resp)
        txt <- httr::content(resp, as = "text", encoding = "UTF-8")
        if (!nzchar(trimws(txt))) break

        dt = tryCatch(
          data.table::fread(text = txt, showProgress = FALSE,
                            na.strings = c("", "NA", "NaN", "null")),
          error = function(e) data.table()
        )
        if (nrow(dt) == 0L) break

        res_list[[k]] = dt
        i = i + 1L
        k = k + 1L
        if (sleep_sec > 0) Sys.sleep(sleep_sec)
      }

      data.table::rbindlist(res_list[seq_len(k - 1L)], fill = TRUE)
    }
    profile = get_company_profile_bulk_all()
    profile = profile[!is.na(symbol)]
    profile = profile[, .(symbol, currency, country, isin, exchange, industry,
                          sector, ipoDate, isEtf, isFund)]
    setnames(profile, "symbol", "fmp_symbol")
    prices[, fmp_symbol := toupper(gsub("\\..*", "", symbol))]
    prices = profile[prices, on = c("fmp_symbol")]
  }

  # Add market cap data
  if (!is.null(market_cap_fmp_file)) {
    mcap = read_parquet(market_cap_fmp_file)
    setnames(mcap, "symbol", "fmp_symbol")
    prices[, fmp_symbol := toupper(gsub("\\..*", "", symbol))]
    prices = mcap[prices, on = c("fmp_symbol", "date")]
    setorder(prices, symbol, date)
    setkey(prices, symbol)
  }

  # Add ETF constituents
  if (!is.null(etf_cons)) {
    print("Add ETF's")
    etf_ = list.files(etf_cons, full.names = TRUE, recursive = TRUE)
    etf_ = lapply(etf_, function(x) {
      # x = etf_[1]
      date_ = tools::file_path_sans_ext(basename(x))
      bn  = basename(dirname(x))
      x = fread(x, col.names = c("symbol", "id", "date", "weight", "mcap"), select = 1:5)
      x[, date := as.character(date)]
      x[is.na(date), date := date_]
      x = unique(x, by = c("symbol", "date"))
      x = x[date == max(date)]
      x[, etf := bn]
      return(x)
    })
    etf_ = rbindlist(etf_)
    etf_[, date := as.Date(date, format = "%Y%m%d")]
    etf_ = unique(etf_, by = c("etf", "symbol", "date"))
    etf_mean = etf_[, .N, by = .(etf, date)] |>
      _[, .(threshold = mean(N) * 0.85), by = etf]
    etf_ = etf_[etf_mean, on = c("etf")]
    etf_[, keep := .N > threshold, by = .(etf, date)]
    etf_ = etf_[keep == TRUE]
    etf_ = etf_[, .(etf, symbol = tolower(symbol), date, index = 1)]
    etf_ = dcast(etf_, symbol + date ~ etf)
    etf_[, symbol_join := symbol]
    etf_[, symbol := NULL]
    prices = prices |>
      dplyr::mutate(symbol_join = gsub("\\..*", "", symbol)) |>
      dplyr::left_join(etf_, by = dplyr::join_by(symbol_join, date))
  }

  return(prices)
}
