generate_target_climatology <- function(targets, # a dataframe already read in
                                 h = 35,
                                 site, # vector of site_ids
                                 model_id = 'climatology',
                                 vars = c("Chla_ugL_mean","Temp_C_mean"),
                                 depth = 'target',
                                 forecast_date = Sys.Date()) {

  if (depth == 'target') {
    # only generates forecasts for target depths
    target_depths <- c(1.5, 1.6, NA)
  } else {
    target_depths <- depth
  }


  # calculate the mean and standard deviation for each doy
  target_clim <- targets %>%
    filter(variable %in% vars,
           depth_m %in% target_depths,
           site_id %in% site) %>%
    mutate(doy = yday(datetime)) %>%
    group_by(doy, site_id, variable, depth_m) %>%
    summarise(clim_mean = mean(observation, na.rm = TRUE),
              clim_sd = sd(observation, na.rm = TRUE),
              .groups = "drop") %>%
    mutate(clim_mean = ifelse(is.nan(clim_mean), NA, clim_mean))

  if (nrow(target_clim) == 0) {
    stop('No targets available. Check that the dates, depths, and sites exist in the target data frame')
  }

  # what dates do we want a forecast of?
  curr_month <- month(forecast_date)
  if(curr_month < 10){
    curr_month <- paste0("0", curr_month)
  }

  curr_year <- year(forecast_date)
  start_date <- forecast_date + days(1)

  forecast_dates <- seq(start_date, as_date(start_date + days(h)), "1 day")
  forecast_doy <- yday(forecast_dates)

  # put in a table
  forecast_dates_df <- tibble(datetime = forecast_dates,
                              doy = forecast_doy)

  forecast <- target_clim %>%
    mutate(doy = as.integer(doy)) %>%
    filter(doy %in% forecast_doy) %>%
    full_join(forecast_dates_df, by = 'doy') %>%
    arrange(site_id, datetime)

  subseted_site_names <- unique(forecast$site_id)
  site_vector <- NULL
  for(i in 1:length(subseted_site_names)){
    site_vector <- c(site_vector, rep(subseted_site_names[i], length(forecast_dates)))
  }

  forecast_tibble <- NULL
  # make sure all are represented
  for (i in 1:length(vars)) {
    forecast_tibble_tmp <- tibble(datetime = rep(forecast_dates, length(subseted_site_names)),
                               site_id = site_vector,
                               variable = vars[i])

    forecast_tibble <- bind_rows(forecast_tibble, forecast_tibble_tmp)
  }

  forecast <- right_join(forecast, forecast_tibble, by = join_by("site_id", "variable", "datetime"))

  # Check for missing and interpolate, remove if there are less than two dates forecasted
  site_count <- forecast %>%
    select(datetime, site_id, variable, clim_mean, clim_sd) %>%
    filter(!is.na(clim_mean)) |>
    group_by(site_id, variable) %>%
    summarize(count = n(), .groups = "drop") |>
    filter(count > 2) |>
    distinct() |>
    pull(site_id)

  combined <- forecast %>%
    filter(site_id %in% site_count) |>
    select(datetime, site_id, depth_m, variable, clim_mean, clim_sd) %>%
    rename(mean = clim_mean,
           sd = clim_sd) %>%
    group_by(site_id, variable) %>%
    mutate(mu = imputeTS::na_interpolation(x = mean),
           sigma = median(sd, na.rm = TRUE)) |>

    # get in standard format
    pivot_longer(c("mu", "sigma"),names_to = "parameter", values_to = "prediction") |>
    mutate(family = "normal") |>
    mutate(reference_datetime = min(datetime) - lubridate::days(1),
           model_id = model_id) |>
    select(model_id, datetime, reference_datetime, site_id, variable, family, parameter, prediction) |>
    mutate(project_id = "vera4cast",
           duration = "P1D")

  return(combined)
}
