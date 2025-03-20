#' Process and Clean dukascopy Tick Quotes data
#'
#' This function processes and cleans raw tick quotes data from Dukascopy.
#'
#' @param dir_path Character. Path to the directory containing the raw price data.
#' @param market Character. Market of the quotes. Default is "ususd".
#' @param symbols Character. Symbols to process. Default is NULL.
#' @param clean Logical. Clean quotes using highfrequency package. Default is FALSE.
#'
#' @return A cleaned and processed data.table with price and return information.
#' @import data.table
#' @importFrom stats na.omit
#' @importFrom data.table .N
#' @import checkmate
#' @importFrom xts as.xts
#' @import highfrequency
#' @importFrom lubridate with_tz
#' @export
dukascopy_quotes = function(dir_path,
                            market = c("ususd"),
                            symbols = NULL,
                            clean = FALSE) {
  # Debug
  # dir_path = "/home/sn/data/equity/us/tick/download"
  # market = "ususd"
  # symbols = "spy"

  EX = SYMBOL = DT = OFRSIZ = BIDSIZ = NULL

  # Validate inputs using checkmate
  assert_directory_exists(dir_path, access = "r")
  assert_character(market, len = 1, any.missing = FALSE)
  assert_logical(clean, len = 1, any.missing = FALSE)

  # Match market argument
  market = match.arg(market)

  # Get files and symbols
  files = list.files(dir_path, full.names = TRUE)
  symbols_ = gsub("-.*", "", basename(files))
  symbols_ = gsub(market, "", symbols_)
  if (!is.null(symbols)) {
    files = files[symbols_ == symbols]
  }

  # Get raw data
  q = lapply(files, fread)
  names(q) = gsub("-.*", "", basename(files))
  q = rbindlist(q, idcol = "symbol")
  setDT(q)

  # Clean quotes using highfrequency package
  if (clean == TRUE) {
    # Change quotes to highfrequncy format
    setnames(q, c("SYMBOL", "DT", "OFR", "BID", "OFRSIZ", "BIDSIZ"))
    q[, EX := "N"] # not sure what N is
    setcolorder(q, colnames(highfrequency::sampleQDataRaw))
    setorder(q, SYMBOL, DT)

    # Fix symbol
    q[, SYMBOL := gsub(market, "", SYMBOL)]

    # Volume to milions
    q[, OFRSIZ := OFRSIZ * 1000000]
    q[, BIDSIZ := BIDSIZ * 1000000]

    # Timezone0
    attributes(q$DT)
    q[, DT := with_tz(DT, tzone = "America/New_York")]

    # Quotes cleaning using highfrequency package
    q = noZeroQuotes(q)
    q = exchangeHoursOnly(q)
    setorder(q, SYMBOL, DT)
    q = rmNegativeSpread(q)
    q = rmLargeSpread(q) # q[, rmLargeSpread(.SD), by = SYMBOL]
    q = mergeQuotesSameTimestamp(q)
    setorder(q, SYMBOL, DT)
    q = rmOutliersQuotes(q)
    setorder(q, SYMBOL, DT)
  }
  return(q)
}
