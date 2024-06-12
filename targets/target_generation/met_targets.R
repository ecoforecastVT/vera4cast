library(tidyverse)
library(RCurl)

## set destination s3 paths
s3 <- arrow::s3_bucket("bio230121-bucket01", endpoint_override = "renc.osn.xsede.org")
#s3$CreateDir("vera4cast/targets/duration=P1D")
#s3$CreateDir("vera4cast/targets/duration=PT1H")

s3_daily <- arrow::s3_bucket("bio230121-bucket01/vera4cast/targets/project_id=vera4cast/duration=P1D", endpoint_override = "renc.osn.xsede.org")
s3_hourly <- arrow::s3_bucket("bio230121-bucket01/vera4cast/targets/project_id=vera4cast/duration=PT1H", endpoint_override = "renc.osn.xsede.org")

column_names <- c("project_id", "site_id","datetime","duration", "depth_m","variable","observation")


# MET TARGETS
print('Met Targets')
current_met <- 'https://raw.githubusercontent.com/FLARE-forecast/FCRE-data/fcre-metstation-data-qaqc/FCRmet_L1.csv'
historic_met <- 'https://pasta.lternet.edu/package/data/eml/edi/389/8/d4c74bbb3b86ea293e5c52136347fbb0'

source('targets/target_functions/meteorology/target_generation_met.R')

met_daily <- target_generation_met(current_met = current_met, historic_met = historic_met, time_interval = 'daily')

met_daily <- met_daily |>
  select(all_of(column_names))

arrow::write_csv_arrow(met_daily, sink = s3_daily$path("daily-met-targets.csv.gz"))

met_hourly <- target_generation_met(current_met = current_met, historic_met = historic_met, time_interval = 'hourly')

met_hourly <- met_hourly |>
  select(all_of(column_names))

arrow::write_csv_arrow(met_hourly, sink = s3_hourly$path("hourly-met-targets.csv.gz"))
