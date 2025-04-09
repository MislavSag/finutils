#ifndef BACKTEST_H
#define BACKTEST_H

#include <Rcpp.h>

double backtest_threshold(Rcpp::NumericVector returns,
                          Rcpp::NumericVector indicator,
                          double threshold,
                          bool sell_below = true);

#endif
