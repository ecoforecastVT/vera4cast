library(dplyr)
library(duckdbfs)
library(minioclient)

config <- yaml::read_yaml("challenge_configuration.yaml")

mc_alias_set("osn", config$endpoint, Sys.getenv("OSN_KEY"), Sys.getenv("OSN_SECRET"))



message('FORECASTS')

# Sync local scores, fastest way to access all the bytes.

mc_mirror(paste0("osn/",config$forecasts_bucket,"/parquet/project_id=",config$project_id),
        paste0("project_id=",config$project_id,"/forecasts"))


# Merely write out locally with new partition via duckdb, fast!
# Sync bytes in bulk again, faster.
fs::dir_create("bundled-parquet/forecasts")

open_dataset(paste0("project_id=",config$project_id,"/forecasts/**")) |>
select(-date) |> # (date is a short version of datetime from partitioning, drop it)
write_dataset(paste0("bundled-parquet/forecasts/project_id=",config$project_id),
              partitioning = c("duration", 'variable', "model_id"))

mc_mirror("bundled-parquet/forecasts/",
        paste0("osn/",config$forecasts_bucket,"/bundled-parquet"))


message('SCORES')

# Sync local scores, fastest way to access all the bytes.

mc_mirror(paste0("osn/",config$scores_bucket,"/parquet/project_id=",config$project_id),
          paste0("project_id=",config$project_id,"/scores"))

# Merely write out locally with new partition via duckdb, fast!
# Sync bytes in bulk again, faster.
fs::dir_create("bundled-parquet/scores")

open_dataset(paste0("project_id=",config$project_id,"/scores/**")) |>
select(-date) |> # (date is a short version of datetime from partitioning, drop it)
write_dataset(paste0("bundled-parquet/scores/project_id=",config$project_id),
              partitioning = c("duration", 'variable', "model_id"))

mc_mirror("bundled-parquet/scores/",
          paste0("osn/",config$scores_bucket,"/bundled-parquet"))


message('SUMMARIES')

# Sync local scores, fastest way to access all the bytes.
mc_mirror(paste0("osn/",config$summaries_bucket,"/project_id=",config$project_id),
          paste0("project_id=",config$project_id,"/summaries"))

# Merely write out locally with new partition via duckdb, fast!
# Sync bytes in bulk again, faster.
fs::dir_create("bundled-parquet/summaries")

open_dataset(paste0("project_id=",config$project_id,"/scores/**")) |>
  select(-date) |> # (date is a short version of datetime from partitioning, drop it)
  write_dataset(paste0("bundled-parquet/summaries/project_id=",config$project_id),
                partitioning = c("duration", 'variable', "model_id"))

mc_mirror("bundled-parquet/summaries/",
          paste0("osn/",config$summaries_bucket,"/bundled-parquet"))

