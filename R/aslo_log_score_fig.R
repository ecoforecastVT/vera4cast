### SCRIPT FOR MAKING LOG SCORE PLOT TO BE USED IN ASLO PRESENTATION
### AUTHOR: AUSTIN DELANY
### CREATED: 2024-05-24

config <- yaml::read_yaml("./challenge_configuration.yaml")
s3_scores <- arrow::s3_bucket(paste0(config$scores_bucket), endpoint_override = config$endpoint, anonymous = TRUE)
sites <- readr::read_csv(paste0("./", config$site_table), show_col_types = FALSE)

df_scores <- arrow::open_dataset(s3_scores) |>
  filter(duration == 'P1D',
         project_id == 'vera4cast') |>
  left_join(sites, by = "site_id") |>
  filter(site_id %in% sites$site_id) |>
  mutate(reference_datetime = lubridate::as_datetime(reference_datetime),
         datetime = lubridate::as_datetime(datetime)) |>
  collect()

googlesheets4::gs4_deauth()
target_metadata <- googlesheets4::read_sheet("https://docs.google.com/spreadsheets/d/1fOWo6zlcWA8F6PmRS9AD6n1pf-dTWSsmGKNpaX3yHNE/edit?usp=sharing")
target_metadata <- target_metadata |>
  rename(variable = `"official" targets name`,
         priority = `priority target`) |>
  filter(duration == 'P1D') |>
  select(variable, class)

df_var_logs <- df_scores |>
  left_join(target_metadata, by = c('variable')) |>
  mutate(horizon = as.numeric(as.Date(date) - as.Date(reference_datetime)))


score_summary <- df_var_logs |>
  filter(!is.infinite(logs),
         !is.nan(logs),
         horizon < 60) |>
  group_by(variable, horizon, class) |>
  summarise(logs = mean(logs, na.rm=TRUE),
            #crps = mean(crps, na.rm=TRUE),
            .groups = "drop") |>
  pivot_longer(cols = c(logs), names_to="metric", values_to="score")
  #collect()

score_summary |>
  #pivot_longer(cols = c(logs), names_to="metric", values_to="score") |>
  #ggplot(aes(x = horizon, y= score,  col=class)) +
  ggplot(aes(x = horizon, y= score, col=variable)) +
  geom_line() +
  facet_wrap(~metric, scales='free') +
  scale_y_log10() +
  theme_bw()

################################

bio_check <- df_scores |> filter(variable %in% c('Bloom_binary_mean', "Chla_ugL_mean")) |>
  left_join(target_metadata, by = c('variable')) |>
  mutate(horizon = as.numeric(as.Date(date) - as.Date(reference_datetime))) |>
  group_by(variable, horizon, class) |>
  summarise(logs = mean(logs, na.rm=TRUE))
