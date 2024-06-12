library(tidyverse)
library(RCurl)

## set destination s3 paths
s3 <- arrow::s3_bucket("bio230121-bucket01", endpoint_override = "renc.osn.xsede.org")
#s3$CreateDir("vera4cast/targets/duration=P1D")
#s3$CreateDir("vera4cast/targets/duration=PT1H")

s3_daily <- arrow::s3_bucket("bio230121-bucket01/vera4cast/targets/project_id=vera4cast/duration=P1D", endpoint_override = "renc.osn.xsede.org")
s3_hourly <- arrow::s3_bucket("bio230121-bucket01/vera4cast/targets/project_id=vera4cast/duration=PT1H", endpoint_override = "renc.osn.xsede.org")

column_names <- c("project_id", "site_id","datetime","duration", "depth_m","variable","observation")

## INFLOWS
print('Inflows')
source('targets/target_functions/inflow/target_generation_inflows.R')

current_inflow <- 'https://raw.githubusercontent.com/FLARE-forecast/FCRE-data/fcre-weir-data-qaqc/FCRWeir_L1.csv'

historic_inflow <- "https://pasta.lternet.edu/package/data/eml/edi/202/12/aae7888d68753b276d1623680f81d5de"

historic_silica <- 'https://pasta.lternet.edu/package/data/eml/edi/542/1/791ec9ca0f1cb9361fa6a03fae8dfc95'

historic_nutrients <- "https://pasta.lternet.edu/package/data/eml/edi/199/12/a33a5283120c56e90ea414e76d5b7ddb"

historic_ghg <- "https://pasta.lternet.edu/package/data/eml/edi/551/8/454c11035c491710243cae0423efbe7b"


inflow_daily <- target_generation_inflows(historic_inflow = historic_inflow,
                                          current_inflow = current_inflow,
                                          historic_nutrients = historic_nutrients,
                                          historic_silica = historic_silica,
                                          historic_ghg = historic_ghg)

inflow_daily <- inflow_daily |> select(column_names)

arrow::write_csv_arrow(inflow_daily, sink = s3_daily$path("daily-inflow-targets.csv.gz"))
