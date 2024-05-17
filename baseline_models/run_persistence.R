print(paste0("Running persistence at ", Sys.time()))

library(tidyverse)
library(lubridate)
library(aws.s3)
library(imputeTS)
library(tsibble)
library(fable)

config <- yaml::read_yaml("challenge_configuration.yaml")
team_name <- 'persistenceRW'

source('R/fablePersistenceModelFunction.R')
source('R/convert2binary.R')

# Read in targets
targets_insitu <- readr::read_csv(paste0("https://", config$endpoint, "/", config$targets_bucket, "/project_id=vera4cast/duration=P1D/daily-insitu-targets.csv.gz"), guess_max = 10000)
targets_met <- readr::read_csv(paste0("https://", config$endpoint, "/", config$targets_bucket, "/project_id=vera4cast/duration=P1D/daily-met-targets.csv.gz"), guess_max = 10000, show_col_types = FALSE)
targets_tubr <- readr::read_csv(paste0("https://", config$endpoint, "/", config$targets_bucket, "/project_id=vera4cast/duration=P1D/daily-inflow-targets.csv.gz"), guess_max = 10000, show_col_types = FALSE)


# Get site information
sites <- readr::read_csv(config$site_table, show_col_types = FALSE)
site_names <- sites$site_id

# Runs the RW forecast for inflow variables
print('Inflow model')

persistenceRW_inflow <- purrr::map_dfr(.x = c('Flow_cms_mean', 'Temp_C_mean'),
                                       .f = ~ generate_baseline_persistenceRW(targets = targets_tubr,
                                                                              h = 35,
                                                                              model_id = 'persistenceRW',
                                                                              forecast_date = Sys.Date(),
                                                                              site = 'tubr',
                                                                              depth = 'target',
                                                                              var = .x,
                                                                              ...))
# met variables
print('Met model')

persistenceRW_met <- generate_baseline_persistenceRW(targets = targets_met,
                                                   h = 35,
                                                   model_id = 'persistenceRW',
                                                   forecast_date = Sys.Date(),
                                                   site = 'fcre',
                                                   depth = 'target',
                                                   var = "AirTemp_C_mean")


# Insitu variables
# get all combinations
print('Insitu model')

site_var_combinations <- expand.grid(var = c('DO_mgL_mean',
                                             'DOsat_percent_mean',
                                             'Chla_ugL_mean',
                                             'Secchi_m_sample',
                                             'Temp_C_mean',
                                             'fDOM_QSU_mean',
                                             #'CH4_umolL_sample',
                                             #'CO2_umolL_sample',
                                             'NH4_ugL_sample',
                                             'DOC_mgL_sample',
                                             'NO3NO2_ugL_sample',
                                             'TP_ugL_sample',
                                             'TN_ugL_sample',
                                             'DIC_mgL_sample'),
                                     site = c('fcre',
                                              'bvre'))

persistenceRW_insitu <- purrr::pmap_dfr(site_var_combinations,
                                      .f = ~ generate_baseline_persistenceRW(targets = targets_insitu,
                                                                         h = 35,
                                                                         model_id = 'persistenceRW',
                                                                         forecast_date = Sys.Date(),
                                                                         depth = 'target',
                                                                         ...))

# Flux variables
# get all combinations
print('Flux model')

site_var_combinations <- expand.grid(var = c('CO2flux_umolm2s_mean',
                                             'CH4flux_umolm2s_mean'),
                                     site = c('fcre'))

persistenceRW_flux <- purrr::pmap_dfr(site_var_combinations,
                                        .f = ~ generate_baseline_persistenceRW(targets = targets_insitu,
                                                                               h = 35,
                                                                               model_id = 'persistenceRW',
                                                                               forecast_date = Sys.Date(),
                                                                               depth = 'target',
                                                                               ...))

# Generate binary forecasts from continuous
binary_site_var_comb <- data.frame(site = c('fcre', 'bvre'),
                                   depth = c(1.6, 1.5))

persistenceRW_insitu_binary <- purrr::pmap_dfr(binary_site_var_comb,
                                             .f = ~convert_continuous_binary(continuous_var = 'Chla_ugL_mean',
                                                                             binary_var = 'Bloom_binary_mean',
                                                                             forecast = persistenceRW_insitu,
                                                                             targets = targets_insitu,
                                                                             threshold = 20,
                                                                             ...))

# combine and submit
combined_persistenceRW <- bind_rows(persistenceRW_inflow, persistenceRW_insitu, persistenceRW_met, persistenceRW_flux, persistenceRW_insitu_binary)

# write forecast file
file_date <- combined_persistenceRW$reference_datetime[1]

forecast_file <- paste0(paste("daily", file_date, team_name, sep = "-"), ".csv.gz")

write_csv(combined_persistenceRW, forecast_file)

combined_persistenceRW %>%
  filter(family == 'normal') |>
  pivot_wider(names_from = parameter, values_from = prediction) |>
  ggplot(aes(x = datetime, y = mu)) +
  geom_line() +
  geom_ribbon(aes(ymax = mu+sigma, ymin = mu-sigma), alpha = 0.3, fill = 'blue') +
  facet_grid(variable~site_id, scales = 'free')

combined_persistenceRW %>%
  filter(family == 'bernoulli') |>
  ggplot(aes(x = datetime, y = prediction, colour = as_factor(depth_m))) +
  geom_line() +
  facet_grid(variable~site_id, scales = 'free')

vera4castHelpers::submit(forecast_file = forecast_file,
                         ask = FALSE,
                         first_submission = FALSE)

unlink(forecast_file)
