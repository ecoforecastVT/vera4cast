print(paste0("Running Creating baselines at ", Sys.time()))

library(tidyverse)
library(lubridate)
library(aws.s3)
library(imputeTS)
library(tsibble)
library(fable)
source('R/MonthlyMeanModelFunction.R')

#' set the random number for reproducible MCMC runs
set.seed(329)

config <- yaml::read_yaml("challenge_configuration.yaml")
team_name <- "monthly_mean"

# where are the relevant targets?
targets_insitu <- readr::read_csv(paste0("https://", config$endpoint, "/", config$targets_bucket, "/project_id=vera4cast/duration=P1D/daily-insitu-targets.csv.gz"), guess_max = 10000)
targets_met <- readr::read_csv(paste0("https://", config$endpoint, "/", config$targets_bucket, "/project_id=vera4cast/duration=P1D/daily-met-targets.csv.gz"), guess_max = 10000, show_col_types = FALSE)
targets_tubr <- readr::read_csv(paste0("https://", config$endpoint, "/", config$targets_bucket, "/project_id=vera4cast/duration=P1D/daily-inflow-targets.csv.gz"), guess_max = 10000, show_col_types = FALSE)

# Get site information
sites <- readr::read_csv(config$site_table, show_col_types = FALSE)
site_names <- sites$site_id

# Inflow variables
monthly_mean_inflow <- purrr::map_dfr(.x = c('Flow_cms_mean', 'Temp_C_mean'),
                                      .f = ~generate_target_monthly_mean(targets = targets_tubr,
                                                                         h = 35,
                                                                         model_id = team_name,
                                                                         forecast_date = Sys.Date(),
                                                                         site = 'tubr', depth = 'target', var = .x))
# Met variables
monthly_mean_met <- generate_target_monthly_mean(targets = targets_met,
                                                 h = 35,
                                                 site = 'fcre',
                                                 var = 'AirTemp_C_mean',
                                                 depth = 'target',
                                                 model_id = team_name,
                                                 forecast_date = Sys.Date())

# Insitu variables
# get all combinations
site_var_combinations <- expand.grid(var = c('DO_mgL_mean',
                                             'DOsat_percent_mean',
                                             'Chla_ugL_mean',
                                             'Secchi_m_sample',
                                             'Temp_C_mean'),
                                     site = c('fcre',
                                              'bvre'))

monthly_mean_insitu <- purrr::pmap_dfr(site_var_combinations,
                                       .f = ~ generate_target_monthly_mean(targets = targets_insitu,
                                                                           h = 35,
                                                                           forecast_date = Sys.Date(),
                                                                           model_id = team_name,
                                                                           depth = 'target', ...))

# combine and submit
combined_monthly_mean <- bind_rows(monthly_mean_met, monthly_mean_inflow, monthly_mean_insitu)

# 4. Write forecast file
file_date <- combined_monthly_mean$reference_datetime[1]

forecast_file <- paste0(paste("daily", file_date, team_name, sep = "-"), ".csv.gz")

write_csv(combined_monthly_mean, forecast_file)

combined_monthly_mean %>%
  pivot_wider(names_from = parameter, values_from = prediction) |>
  ggplot(aes(x = datetime, y = mu)) +
  geom_line() +
  geom_ribbon(aes(ymax = mu+sigma, ymin = mu-sigma), alpha = 0.3, fill = 'blue') +
  facet_grid(variable~site_id, scales = 'free')

vera4castHelpers::submit(forecast_file = forecast_file,
                         ask = FALSE,
                         first_submission = FALSE)

unlink(forecast_file)
