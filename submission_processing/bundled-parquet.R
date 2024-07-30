library(dplyr)
library(duckdbfs)
library(minioclient)
library(bench)

mc_alias_set("osn", "renc.osn.xsede.org", Sys.getenv("OSN_KEY"), Sys.getenv("OSN_SECRET"))


message('FORECASTS')

# Sync local scores, fastest way to access all the bytes.
bench::bench_time({ # 13.7s

  mc_mirror("osn/bio230121-bucket01/vera4cast/forecasts/parquet/project_id=vera4cast",
            "project_id=vera4cast/forecasts")
})

# Merely write out locally with new partition via duckdb, fast!
# Sync bytes in bulk again, faster.
fs::dir_create("bundled-parquet/forecasts")
bench::bench_time({ # 34.38s

  open_dataset("project_id=vera4cast/forecasts/**") |>
    select(-date) |> # (date is a short version of datetime from partitioning, drop it)
    write_dataset("bundled-parquet/forecasts/project_id=vera4cast",
                  partitioning = c("duration", 'variable', "model_id"))

  mc_mirror("bundled-parquet/forecasts/",
            "osn/bio230121-bucket01/vera4cast/forecasts/bundled-parquet")
})


message('SCORES')

# Sync local scores, fastest way to access all the bytes.
bench::bench_time({ # 13.7s

  mc_mirror("osn/bio230121-bucket01/vera4cast/scores/parquet/project_id=vera4cast",
            "project_id=vera4cast/scores")
})

# Merely write out locally with new partition via duckdb, fast!
# Sync bytes in bulk again, faster.
fs::dir_create("bundled-parquet/scores")
bench::bench_time({ # 34.38s

  open_dataset("project_id=vera4cast/scores/**") |>
    select(-date) |> # (date is a short version of datetime from partitioning, drop it)
    write_dataset("bundled-parquet/scores/project_id=vera4cast",
                  partitioning = c("duration", 'variable', "model_id"))

  mc_mirror("bundled-parquet/scores/",
            "osn/bio230121-bucket01/vera4cast/scores/bundled-parquet")
})


message('SUMMARIES')

# Sync local scores, fastest way to access all the bytes.
bench::bench_time({ # 13.7s

  mc_mirror("osn/bio230121-bucket01/vera4cast/forecasts/summaries/project_id=vera4cast",
            "project_id=vera4cast/summaries")
})

# Merely write out locally with new partition via duckdb, fast!
# Sync bytes in bulk again, faster.
fs::dir_create("bundled-parquet/summaries")
bench::bench_time({ # 34.38s

  open_dataset("project_id=vera4cast/summaries/**") |>
    select(-date) |> # (date is a short version of datetime from partitioning, drop it)
    write_dataset("bundled-parquet/summaries/project_id=vera4cast",
                  partitioning = c("duration", 'variable', "model_id"))

  mc_mirror("bundled-parquet/summaries/",
            "osn/bio230121-bucket01/vera4cast/forecasts/summaries/bundled-parquet")
})


## We are done.


## direct write, much slower...
#bench::bench_time({
#  scores |> write_dataset("s3://bio230014-bucket01/challenges/scores/bundled-parquet/project_id=neon4cast",
#                          partitioning = c("duration", 'variable', "model_id"),
#                          s3_endpoint = "sdsc.osn.xsede.org",
#                          s3_access_key_id = Sys.getenv("OSN_KEY"),
#                          s3_secret_access_key=Sys.getenv("OSN_SECRET"))
#})



# # TESTING: single URL is fast
# url <- paste0("https://renc.osn.xsede.org/bio230121-bucket01/vera4cast/",
#               "scores/bundled_scores/project_id=vera4cast/duration=P1D/",
#               "variable=Temp_C_mean/model_id=climatology/date=2024-07-29/part-0.parquet")
# bench::bench_time({ # 1.69s
#   duckdbfs::open_dataset(url) |> collect()
# })
#
#
# ## Testing, inventory computation is fast
# s3 <- paste0("s3://anonymous@bio230121-bucket01/vera4cast/scores/parquet/bundled-parquet/",
#              "project_id=vera4cast/duration=P1D/variable=Temp_C_mean")
#
# bench::bench_time({ # 3.43s
#   open_dataset(s3, s3_endpoint = "renc.osn.xsede.org") |>
#     count(model_id, datetime) |>
#     collect()
# })
