print(paste0("Running Creating baselines at ", Sys.time()))

library(tidyverse)
library(lubridate)
library(aws.s3)
library(imputeTS)
library(tsibble)
library(fable)

source('R/ClimatologyModelFunction.R')
source('R/convert2binary.R')

#' set the random number for reproducible MCMC runs
set.seed(329)

config <- yaml::read_yaml("challenge_configuration.yaml")
team_name <- "climatology"

# where are the relevant targets?
targets_insitu <- readr::read_csv(paste0("https://", config$endpoint, "/", config$targets_bucket, "/project_id=vera4cast/duration=P1D/daily-insitu-targets.csv.gz"), guess_max = 10000)
targets_met <- readr::read_csv(paste0("https://", config$endpoint, "/", config$targets_bucket, "/project_id=vera4cast/duration=P1D/daily-met-targets.csv.gz"), guess_max = 10000, show_col_types = FALSE)
targets_tubr <- readr::read_csv(paste0("https://", config$endpoint, "/", config$targets_bucket, "/project_id=vera4cast/duration=P1D/daily-inflow-targets.csv.gz"), guess_max = 10000, show_col_types = FALSE)

# Get site information
sites <- readr::read_csv(config$site_table, show_col_types = FALSE)
site_names <- sites$site_id

# Inflow variables
climatology_inflow <- purrr::map_dfr(.x = c('Flow_cms_mean', 'Temp_C_mean'),
                                     .f = ~ generate_baseline_climatology(targets = targets_tubr,
                                                                          h = 35,
                                                                          forecast_date = Sys.Date(),
                                                                          site = 'tubr', depth = 'target', var = .x))
# Met variables
climatology_met <- generate_baseline_climatology(targets = targets_met,
                                               h = 35,
                                               site = 'fcre',
                                               var = 'AirTemp_C_mean',
                                               depth = 'target',
                                               forecast_date = Sys.Date())

# Insitu variables
# get all combinations
site_var_combinations <- expand.grid(var = c('DO_mgL_mean',
                                             'DOsat_percent_mean',
                                             'Chla_ugL_mean',
                                             'Secchi_m_sample',
                                             'Temp_C_mean',
                                             'fDOM_QSU_mean',
                                             'CH4_umolL_sample',
                                             'CO2_umolL_sample'),
                                     site = c('fcre',
                                              'bvre'))

climatology_insitu <- purrr::pmap_dfr(site_var_combinations,
                                      .f = ~ generate_baseline_climatology(targets = targets_insitu,
                                                                         h = 35,
                                                                         forecast_date = Sys.Date(),
                                                                         depth = 'target',
                                                                         ...))

# Flux variables
# get all combinations
print('Flux model')

climatology_flux <- purrr::map_dfr(.x = c('CO2flux_umolm2s_mean', 'CH4flux_umolm2s_mean'),
               .f = ~ generate_baseline_climatology(targets = targets_insitu,
                                                    h = 35,
                                                    forecast_date = Sys.Date(),
                                                    site = 'fcre', depth = 'target', var = .x))

# Generate binary forecasts from continuous
binary_site_var_comb <- data.frame(site = c('fcre', 'bvre'),
                                   depth = c(1.6, 1.5))

climatology_insitu_binary <- purrr::pmap_dfr(binary_site_var_comb,
                                             .f = ~convert_continuous_binary(continuous_var = 'Chla_ugL_mean',
                                                                             binary_var = 'Bloom_binary_mean',
                                                                             forecast = climatology_insitu,
                                                                             targets = targets_insitu,
                                                                             threshold = 20,
                                                                             ...))

# combine and submit
combined_climatology <- bind_rows(climatology_met, climatology_inflow, climatology_insitu, climatology_insitu_binary, climatology_flux)

# 4. Write forecast file
file_date <- combined_climatology$reference_datetime[1]

forecast_file <- paste0(paste("daily", file_date, team_name, sep = "-"), ".csv.gz")

write_csv(combined_climatology, forecast_file)

combined_climatology %>%
  filter(family == 'normal') |>
  pivot_wider(names_from = parameter, values_from = prediction) |>
  ggplot(aes(x = datetime, y = mu, fill = as_factor(depth_m))) +
  geom_line() +
  geom_ribbon(aes(ymax = mu+sigma, ymin = mu-sigma), alpha = 0.3, fill = 'blue') +
  facet_grid(variable~site_id, scales = 'free')

combined_climatology %>%
  filter(family == 'bernoulli') |>
  ggplot(aes(x = datetime, y = prediction, colour = as_factor(depth_m))) +
  geom_line() +
  facet_grid(variable~site_id, scales = 'free')

vera4castHelpers::submit(forecast_file = forecast_file,
                         ask = FALSE,
                         first_submission = FALSE)

unlink(forecast_file)
