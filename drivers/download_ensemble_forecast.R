
download_ensemble_forecast <- function(model, forecast_horizon = 35, sites = NULL){

  s3 <- arrow::s3_bucket(bucket = "bio230121-bucket01/flare",
                         endpoint_override = "amnh1.osn.mghpcc.org",
                         access_key = Sys.getenv("OSN_KEY"),
                         secret_key = Sys.getenv("OSN_SECRET"))

  s3$CreateDir("drivers/met/ensemble_forecast")

  s3 <- arrow::s3_bucket("bio230121-bucket01/flare/drivers/met/ensemble_forecast",
                         endpoint_override = "amnh1.osn.mghpcc.org",
                         access_key = Sys.getenv("OSN_KEY"),
                         secret_key = Sys.getenv("OSN_SECRET"))

  print('Created s3 connections...')

  site_list <- readr::read_csv("https://raw.githubusercontent.com/FLARE-forecast/aws_noaa/master/site_list_v2.csv", show_col_types = FALSE)

  if(!is.null(sites)){
    site_list <- site_list |> dplyr::filter(site_id %in% sites)
  }

  for(i in 1:nrow(site_list)){

    print(site_list$site_id[i])

    ropenmeteo::get_ensemble_forecast(
      latitude = site_list$latitude[i],
      longitude = site_list$longitude[i],
      site_id = site_list$site_id[i],
      forecast_days = forecast_horizon,
      past_days = 0,
      model = model,
      variables = ropenmeteo::glm_variables(product = "ensemble_forecast",
                                            time_step = "hourly")) |>
      dplyr::mutate(reference_date = lubridate::as_date(reference_datetime)) |>
      arrow::write_dataset(s3, format = 'parquet',
                           partitioning = c("model_id", "reference_date", "site_id"))

    print(paste(site_list$site_id[i],"forecast saved..."))
    Sys.sleep(30)

  }
}
