#include <Rcpp.h>
#include "backtest.h"
using namespace Rcpp;



DataFrame cbind_scalar_and_df(double x, DataFrame df) {
  int n = df.nrows();
  NumericVector new_col(n, x);
  List new_df(df.size() + 1);

  new_df[0] = new_col;
  CharacterVector df_names = df.names();
  for (int i = 0; i < df.size(); i++) {
    new_df[i + 1] = df[i];
  }

  CharacterVector new_names(df.size() + 1);
  new_names[0] = "x";
  for (int i = 0; i < df.size(); i++) {
    new_names[i + 1] = df_names[i];
  }

  new_df.attr("names") = new_names;
  new_df.attr("class") = "data.frame";
  new_df.attr("row.names") = df.attr("row.names");

  return DataFrame(new_df);
}

DataFrame bind_scalar_and_vector_to_df(double x, NumericVector vec) {
  int n = vec.size();
  NumericVector scalar_vec(n, x);
  DataFrame result = DataFrame::create(Named("Scalar") = scalar_vec, Named("Vector") = vec);
  return result;
}

DataFrame sliceDataFrameColumnWise(DataFrame df, int start, int end) {
  int n = df.size();
  List sliced(n);

  for (int i = 0; i < n; i++) {
    if (TYPEOF(df[i]) == REALSXP) {
      NumericVector col = as<NumericVector>(df[i]);
      sliced[i] = col[Range(start, end)];
    } else {
      Rcpp::Rcout << "Non-numeric column encountered at index: " << i << std::endl;
    }
  }

  DataFrame result(sliced);
  result.attr("names") = df.attr("names");
  result.attr("row.names") = df.attr("row.names");

  return result;
}

//' Run Backtest Without SMA
//'
//' Processes a DataFrame and parameter DataFrame to compute backtest results using
//' a unified backtesting function. The function applies the given threshold logic to each
//' indicator column specified in `params`.
//'
//' @param df DataFrame containing at least the `returns` column and indicator columns.
//' @param params DataFrame with parameters, including columns "variable" (indicator names)
//'   and "thresholds" (numeric thresholds).
//' @param sell_below Logical flag; if TRUE, sells when the indicator is below the threshold.
//' @return A numeric vector with backtest results for each row in `params`.
//' @export
// [[Rcpp::export]]
 NumericVector opt_threshold(DataFrame df, DataFrame params, bool sell_below = true) {
   int n_params = params.nrow();
   NumericVector returns = df["returns"];
   CharacterVector indicators = params["variable"];
   NumericVector thresholds = params["thresholds"];
   NumericVector results(n_params);

   for (int i = 0; i < n_params; ++i) {
     String var_name = indicators[i];
     NumericVector indicator = df[var_name];
     results[i] = backtest_threshold(returns, indicator, thresholds[i], sell_below);
   }

   return results;
 }

//' Calculate Simple Moving Average (SMA)
//'
//' Computes the Simple Moving Average (SMA) for the numeric vector `x` using
//' a window of size `n`. For indices with insufficient data points, NA is returned.
//'
//' @param x Numeric vector for which the SMA is calculated.
//' @param n Window size (number of observations) used for the SMA calculation.
//' @return A numeric vector of the same length as `x` containing the SMA values.
//' @export
// [[Rcpp::export]]
NumericVector calculate_sma(NumericVector x, int n) {
  int size = x.size();
  NumericVector sma(size);
  double sum = 0.0;

  for (int i = 0; i < size; i++) {
    sum += x[i];
    if (i >= n) {
      sum -= x[i - n];
    }
    if (i >= n - 1) {
      sma[i] = sum / n;
    } else {
      sma[i] = NA_REAL;
    }
    }

   return sma;
}

//' Run Backtest with SMA Filtering
//'
//' Applies a simple moving average (SMA) filter to indicator columns before performing backtesting.
//' For each parameter row in `params`, the function calculates the SMA of the corresponding indicator
//' and then computes the backtest result using the unified backtesting function.
//'
//' @param df DataFrame containing at least the `returns` column and indicator columns.
//' @param params DataFrame with parameters, including "variable", "thresholds", and "sma_n" (window size for SMA).
//' @param sell_below Logical flag; if TRUE, sells when the SMA-filtered indicator is below the threshold.
//' @return A numeric vector of backtest results for each parameter set after SMA filtering.
//' @export
// [[Rcpp::export]]
NumericVector opt_with_sma_threshold(DataFrame df, DataFrame params, bool sell_below = true) {
  int n_params = params.nrow();
  NumericVector returns = df["returns"];
  CharacterVector indicators = params["variable"];
  NumericVector thresholds = params["thresholds"];
  NumericVector sma_n = params["sma_n"];
  NumericVector results(n_params);

  for (int i = 0; i < n_params; i++) {
    String col_name = indicators[i];
    NumericVector indicator = df[col_name];
    int sma_val = sma_n[i];
    NumericVector sma_indicator = calculate_sma(indicator, sma_val);
    results[i] = backtest_threshold(returns, sma_indicator, thresholds[i], sell_below);
    }

  return results;
}

//' Combined Windowed Optimization with SMA Filtering
//'
//' Applies a window-based optimization to a DataFrame by slicing it into windows (either rolling or expanding)
//' and then performing SMA-based backtesting on each slice using [opt_with_sma_threshold]. The output is a list of
//' data frames binding the time stamp with the backtest results for each window.
//'
//' @param df DataFrame containing the data. Must include a `time` column and a `returns` column.
//' @param params DataFrame with parameters; expected to include columns "variable", "thresholds", and "sma_n".
//' @param window Integer specifying the window size (number of rows) to use for each optimization slice.
//' @param window_type A string to determine the window slicing method; either "rolling" (default) or "expanding".
//' @param sell_below Logical flag; if TRUE, sells when the SMA-filtered indicator is below the threshold.
//' @return A list of DataFrames, each with a time stamp and the corresponding backtest results for that window.
//' @export
// [[Rcpp::export]]
List wfo_combined(DataFrame df,
                   DataFrame params,
                   int window,
                   std::string window_type = "rolling",
                   bool sell_below = true) {
   int n = df.nrow();
   NumericVector time = df["time"];

   if (window > n) {
     stop("Window size exceeds available data.");
   }

   int num_windows = n - window + 1;
   List results(num_windows);

   for (int i = 0; i < num_windows; ++i) {
     DataFrame df_window;
     if (i + window - 1 < n) {
       if (window_type == "expanding") {
         df_window = sliceDataFrameColumnWise(df, 0, i + window - 1);
       } else {
         df_window = sliceDataFrameColumnWise(df, i, i + window - 1);
       }
     } else {
       Rcpp::Rcout << "Skipping window " << i
                   << " due to index out of range." << std::endl;
       continue;
     }

     NumericVector sr;
     sr = opt_with_sma_threshold(df_window, params, sell_below);
     int time_index = i + window - 1;
     results[i] = bind_scalar_and_vector_to_df(time[time_index], sr);
   }

   return results;
}
