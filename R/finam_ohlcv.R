#' Process and Clean Dukascopy Tick Quotes data
#'
#' This function processes and cleans raw tick quotes data from Dukascopy.
#'
#' @param dir_path A character string with the path to the directory containing the raw tick quotes data.
#' @param symbols A character string with the symbol to process.
#'     If NULL, all symbols in the directory will be processed.
#' @param clean A logical value indicating whether to clean the data using the
#'     highfrequency package. Default is FALSE.
#'
#' @return A cleaned and processed data.table with price and return information.
#' @import data.table
#' @importFrom stats na.omit
#' @importFrom data.table .N
#' @import checkmate
#' @importFrom xts as.xts
#' @import highfrequency
#' @importFrom lubridate with_tz
#' @importFrom arrow read_parquet
#' @export
finam_ohlcv = function(dir_path, symbols, clean = FALSE) {
  # Debug
  # path = "/home/sn/data/equity/us/tick"
  # symbols = "spy"

  EX = SYMBOL = DT = OFRSIZ = BIDSIZ = COND = CORR = NULL

  # Get files and symbols
  files_dir = list.files(dir_path, full.names = TRUE)
  symbols_ = basename(files_dir)
  if (!is.null(symbols)) {
    files_dir = files_dir[symbols_ == symbols]
  }

  # Get raw data
  trades = list()
  for (i in seq_along(files_dir)) {
    f = files_dir[i]
    files = list.files(f, full.names = TRUE)
    trades[[i]] = rbindlist(lapply(files, read_parquet))
  }
  names(trades) = basename(files_dir)
  trades = rbindlist(trades, idcol = "symbol")

  # clean trades using highfrequency package
  if (clean == TRUE) {
    # Change quotes to highfrequncy format
    setnames(trades, c("SYMBOL", "DT", "PRICE", "SIZE"))
    trades[, EX := "N"] # not sure what N is
    trades[, COND := "F"] # not sure uf this is important
    trades[, CORR := 0]
    setcolorder(trades, colnames(highfrequency::sampleTDataRaw))
    setorder(trades, SYMBOL, DT)

    # Timezone
    # x = trades[DT %between% c(as.POSIXct("2025-01-03 05:00:00", tz = "UTC"), as.POSIXct("2025-01-03 22:00:00", tz = "UTC"))]
    # highfrequency::aggregateTrades(x, alignBy = "hours", alignPeriod = 1, marketOpen = "07:00", marketClose = "16:00")
    trades[, DT := with_tz(DT, tzone = "America/New_York")]
    attributes(trades$DT)

    # Quotes cleaning using highfrequency package
    mergeTradesSameTimestamp

    trades = noZeroPrices(trades)
    trades = exchangeHoursOnly(trades)
    setorder(trades, SYMBOL, DT)
    trades = mergeTradesSameTimestamp(trades)
    setorder(trades, SYMBOL, DT)
  }
  return(trades)
}
