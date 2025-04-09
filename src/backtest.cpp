#include <Rcpp.h>
#include "backtest.h"
using namespace Rcpp;


//' Backtest Threshold Function
//'
//' This function computes cumulative returns.
//'
//' @param returns A numeric vector of returns.
//' @param indicator A numeric vector used as an indicator.
//' @param threshold A numeric threshold.
//' @param sell_below Logical, indicating sell decision.
//' @return A single numeric value corresponding to the adjusted cumulative return.
//' @export
// [[Rcpp::export]]
double backtest_threshold(NumericVector returns,
                          NumericVector indicator,
                          double threshold,
                          bool sell_below) {  // Remove "= true" here
  int n = indicator.size();
  NumericVector sides(n, 1.0); // Initialize sides with 1

  // Loop from the second period onward (we look back one period)
  for (int z = 1; z < n; ++z) {
    if (!NumericVector::is_na(indicator[z - 1])) {
      // If sell_below is true, sell if indicator falls below the threshold.
      // Otherwise, sell if the indicator is above the threshold.
      if ((sell_below && indicator[z - 1] < threshold) ||
          (!sell_below && indicator[z - 1] > threshold)) {
        sides[z] = 0;
      }
    }
  }

  // Calculate cumulative returns
  double cum_returns = 1.0;
  for (int z = 0; z < n; ++z) {
    cum_returns *= (1 + returns[z] * sides[z]);
  }

  return cum_returns - 1; // Return adjusted cumulative return
}
