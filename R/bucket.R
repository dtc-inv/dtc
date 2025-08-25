check_api_keys <- function(fpath = NULL) {
  if (!exists("api_keys")) {
    if (is.null(fpath)) {
      fpath <- "~/api_keys.RData"
    }
    load(fpath)
  }
}

#' @import arrow
create_bucket <- function(api_keys) {
  s3_bucket(
    bucket = "dtc-par",
    access_key = api_keys$s3$access_key,
    secret_key = api_keys$s3$secret_key
  )
}


try_read <- function(bucket, fpath, max_iter = 10) {
  for (i in 1:max_iter) {
    x <- try(read_parquet(bucket$path(fpath)))
    if (inherits(x, "data.frame")) {
      return(x)
    }
  }
}

try_write <- function(bucket, dat, fpath, max_iter = 10) {
  for (i in 1:max_iter) {
    x <- try(write_parquet(dat, bucket$path(fpath)))
    if (!inherits(x, "try-error")) {
      return("success")
    }
  }
}

read_msl <- function(bucket) {
  try_read(bucket, "meta-tables/tbl_msl.parquet")
}
