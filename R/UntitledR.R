t <- arrow::s3_bucket('bio230121-bucket01/vera4cast/forecasts/parquet/project_id=vera4cast/duration=P1D/variable=TP_ugL_sample/model_id=inflow_gefsClimAED/reference_date=2024-04-03',
                      endpoint_override = "renc.osn.xsede.org",
                      anonymous=TRUE)

v <- arrow::open_dataset(t) |>
  collect()
