#' Save data to Azure Blob Storage
#'
#' @param x Data to save
#' @param filename Name of the file to save
#' @param container_name Name of the container to save the file to
#' @param endpoint Azure Blob Storage endpoint
#' @param key Azure Blob Storage key
#' @return 1
#' @export
save_azure_blob = function(x,
                           filename,
                           container_name,
                           endpoint = Sys.getenv("ENDPOINT"),
                           key = Sys.getenv("KEY")) {
  blobendpoint = storage_endpoint(endpoint, key=key)
  cont = storage_container(blobendpoint, container_name)
  if (tools::file_ext(".csv")) {
    storage_write_csv(x, cont, file_name)
    print("Csv saved")
  }
  return(1)
}
