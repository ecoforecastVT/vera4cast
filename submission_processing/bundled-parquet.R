library(dplyr)
library(duckdbfs)
library(minioclient)

install_mc()

config <- yaml::read_yaml("challenge_configuration.yaml")

mc_alias_set("osn", config$endpoint, Sys.getenv("OSN_KEY"), Sys.getenv("OSN_SECRET"))



message('FORECASTS')

# Sync local scores, fastest way to access all the bytes.

mc_mirror(paste0("osn/",config$forecasts_bucket,"/parquet/project_id=",config$project_id),
        paste0("/project_id=",config$project_id,"/forecasts"))


# Merely write out locally with new partition via duckdb, fast!
# Sync bytes in bulk again, faster.
fs::dir_create(paste0("bundling-forecasts"))

open_dataset(paste0("/project_id=",config$project_id,"/forecasts/**")) |>
#select(-date) |> # (date is a short version of datetime from partitioning, drop it)
write_dataset(paste0("bundling-forecasts/project_id=",config$project_id),
              partitioning = c("duration", 'variable', "model_id"))

mc_mirror("bundling-forecasts",
        paste0("osn/",config$forecasts_bucket,"/bundled-parquet"))


message('SCORES')

# Sync local scores, fastest way to access all the bytes.

mc_mirror(paste0("osn/",config$scores_bucket,"/parquet/project_id=",config$project_id),
          paste0("/project_id=",config$project_id,"/scores"))

# Merely write out locally with new partition via duckdb, fast!
# Sync bytes in bulk again, faster.
fs::dir_create("bundling/scores")

open_dataset(paste0("/project_id=",config$project_id,"/scores/**")) |>
select(-date) |> # (date is a short version of datetime from partitioning, drop it)
write_dataset(paste0("bundling/scores/project_id=",config$project_id),
              partitioning = c("duration", 'variable', "model_id"))

mc_mirror("bundling/scores/",
          paste0("osn/",config$scores_bucket,"/bundled-parquet"))


message('SUMMARIES')

# Sync local scores, fastest way to access all the bytes.
mc_mirror(paste0("osn/",config$summaries_bucket,"/project_id=",config$project_id),
          paste0("/project_id=",config$project_id,"/summaries"))

# Merely write out locally with new partition via duckdb, fast!
# Sync bytes in bulk again, faster.
fs::dir_create("bundling/summaries")

open_dataset(paste0("/project_id=",config$project_id,"/summaries/**")) |>
  #select(-date) |> # (date is a short version of datetime from partitioning, drop it)
  write_dataset(paste0("bundling/summaries/project_id=",config$project_id),
                partitioning = c("duration", 'variable', "model_id"))

mc_mirror("bundling/summaries/",
          paste0("osn/",config$forecasts_bucket,"/bundled-summaries"))

