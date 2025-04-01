#' Set ggplot2 options
#'
#' Set ggplot2 options for discrete colour and fill.
#'
#' @export
set_options = function() {
  options(
    ggplot2.discrete.colour = c("#D55E00", "#0072B2","#009E73", "#CC79A7", "#E69F00", "#56B4E9", "#F0E442"),
    ggplot2.discrete.fill = c("#D55E00", "#0072B2","#009E73", "#CC79A7", "#E69F00", "#56B4E9", "#F0E442")
  )
}
