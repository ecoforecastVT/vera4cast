## SCRIPT TO SHOW HOW TO ACCESS AND SUBSET DATA FROM S3 ##
library(dplyr)
library(arrow)

## USE THE R `arrow` package to access data from an S3 bucket ##
all_results <- arrow::open_dataset("s3://anonymous@bio230121-bucket01/vera4cast/scores/bundled-parquet/project_id=vera4cast/duration=P1D/variable=Temp_C_mean/model_id=glm_aed_v1?endpoint_override=renc.osn.xsede.org")
df <- all_results |> dplyr::collect()

## QUERY CAN ALSO BE ORGANIZED WITH `dplyr` PIPE COMMANDS ##
## this allows you to subset beyond the partitions included in the parquet path below (think datetime, site_id, etc.)
## query times will vary depending on how many filters are added
all_results <- arrow::open_dataset("s3://anonymous@bio230121-bucket01/vera4cast/scores/bundled-parquet/project_id=vera4cast?endpoint_override=renc.osn.xsede.org")
df <- all_results |>
  dplyr::filter(duration == 'P1D', # this indicates only daily forecasts (we also have hourly forecasts for some variables)
                variable == 'Temp_C_mean',
                model_id == 'glm_aed_v1',
                site_id == 'fcre',
                reference_datetime > lubridate::as_datetime('2024-09-20')) |> # only grab forecasts after Sept 20th
  dplyr::collect() # use collect to pull data from the s3 bucket after you are done subsetting


## This can be done for all groups of forecast output (forecasts, scores, forecast summaries) by adjusting the s3 path
## Forecasts and Forecast Summaries will likely take longer to download because they contain all forecast ensemble members

# FORECASTS
all_results <- arrow::open_dataset("s3://anonymous@bio230121-bucket01/vera4cast/forecasts/bundled-parquet/project_id=vera4cast/duration=P1D/variable=Temp_C_mean/model_id=glm_aed_v1?endpoint_override=renc.osn.xsede.org")
df <- all_results |> dplyr::collect()

# FORECAST SUMMARIES
all_results <- arrow::open_dataset("s3://anonymous@bio230121-bucket01/vera4cast/forecasts/bundled-summaries/project_id=vera4cast/duration=P1D/variable=Temp_C_mean/model_id=glm_aed_v1?endpoint_override=renc.osn.xsede.org")
df <- all_results |> dplyr::collect()
