source("drivers/download_ensemble_forecast.R")
source("drivers/submit_met_forecast.R")

model_id <- "gfs_seamless"
download_ensemble_forecast(model = model_id)
submit_met_forecast(model_id)

model_id <- "icon_seamless"
download_ensemble_forecast(model_id, forecast_horizon = 7, sites = "fcre")
submit_met_forecast(model_id)

model_id <- "gem_global"
download_ensemble_forecast(model_id, forecast_horizon = 32, sites = "fcre")
submit_met_forecast(model_id)

model_id <- "ecmwf_ifs04"
download_ensemble_forecast(model_id, forecast_horizon = 10, sites = "fcre")
submit_met_forecast(model_id)


## Call healthcheck
#RCurl::url.exists("https://hc-ping.com/d39f878d-2dc0-4373-ab01-22fa417692f8", timeout = 5)
