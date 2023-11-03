# vera4castHelpers

Tools for validating file formats and submitting to the VERA forecasting challenge

## Installation

```
remotes::install_github("/LTREB-reservoirs/vera4castHelpers")
```

### Usage

```
vera4castHelpers::submit(forecast_file = "test.csv")
```

If you will be submitting multiple forecasts using the same model_id, use the following

```
vera4castHelpers::submit(forecast_file = "test.csv",
                         first_submission = FALSE)
```

