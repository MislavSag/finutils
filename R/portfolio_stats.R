#' Portfolio Performance statistics
#'
#' This function calculates portfolio performance statistics such as cumulative return,
#' annualized return, Sharpe ratio, adjusted Sharpe ratio, maximum drawdown,
#' maximum drawdown length, and beta.
#'
#' @param returns xts. A time series object with returns for each asset in the portfolio.
#' @param market xts. A time series object with returns for the market index.
#'
#' @return A cleaned and processed data.table with price and return information.
#' @import data.table
#' @import xts
#' @importFrom stats na.omit
#' @importFrom data.table .N
#' @import checkmate
#' @export
portfolio_stats = function(returns,
                           market = NULL) {

  symbol = high = low = volume = adj_close = n = symbol_short = adj_rate =
     N = `.` = dollar_vol_rank = close_raw = NULL

  # Debug
  # library(PerformanceAnalytics)
  # library(data.table)
  # dt = finutils::qc_daily(
  #   file_path = "F:/lean/data/stocks_daily.csv",
  #   symbols = c("tlt", "spy")
  # )
  # dt[, returns := close / shift(close) - 1, by = symbol]
  # returns = dcast(dt[, .(symbol, date, returns)], date ~ symbol, value.var = "returns")
  # returns = na.omit(returns)
  # returns = as.xts.data.table(returns)
  # market = returns[, "spy"]

  # Performance analytics
  ret_cum = Return.cumulative(returns)
  ret_ann = Return.annualized(returns)
  sr      = SharpeRatio.annualized(returns)
  asr     = AdjustedSharpeRatio(returns)

  # DDs
  dds = lapply(returns, function(r) sortDrawdowns(findDrawdowns(r)))
  dd_max_loss = t(as.matrix(sapply(dds, function(x) min(x$return))))
  rownames(dd_max_loss) = "Max Drawdown"
  dd_max_length = t(as.matrix(sapply(dds, function(x) max(x$length))))
  rownames(dd_max_length) = "Max Drawdown Length"

  # No rownames
  if (!is.null(market)) {
    beta = lapply(returns, function(r) {
      BetaCoVariance(r, market)
    })
    beta = t(as.matrix(unlist(beta)))
    rownames(beta) = "Beta"
  }
  # PerformanceAnalytics::MartinRatio(returns)

  # Combine
  portfolio_stats = rbind(ret_cum, ret_ann, sr, asr,
                          dd_max_loss, dd_max_length, beta)
  portfolio_stats = apply(portfolio_stats, 2, round, 4)

  return(portfolio_stats)
}
