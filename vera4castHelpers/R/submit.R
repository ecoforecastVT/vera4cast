#' Submit forecast to forecasting challenge
#'
#' @inheritParams forecast_output_validator
#' @param ask should we prompt for a go before submission?
#' @param s3_region subdomain of submission bucket
#' @param s3_endpoint root domain of submission bucket
#' @param first_submission flag if first submission.  set to FALSE if submitting multiple forecasts
#' @export
submit <- function(forecast_file,
                   ask = NULL,
                   s3_region = "submit",
                   s3_endpoint = "ltreb-reservoirs.org",
                   first_submission = TRUE
){
  if(file.exists("~/.aws")){
    warning(paste("Detected existing AWS credentials file in ~/.aws,",
                  "Consider renaming these so that automated upload will work"))
  }
  message("validating that file matches required standard")
  go <- forecast_output_validator(forecast_file)

  googlesheets4::gs4_deauth()
  message("Checking if model_id is registered")
  registered_model_id <- suppressMessages(googlesheets4::read_sheet("https://docs.google.com/spreadsheets/d/1f177dpaxLzc4UuQ4_SJV9JWIbQPlilVnEztyvZE6aSU/edit?usp=sharing", range = "Sheet1!A:A"))

  df <- readr::read_csv(forecast_file, show_col_types = FALSE)
  model_id <- df$model_id[1]

if(grepl("(example)", model_id)){
  message(paste0("You are submitting a forecast with 'example' in the model_id. As a example forecast, it will be processed but only retained for 30-days.\n",
          "No registration is required to submit an example forecast.\n",
          "If you want your forecast to be retained, please select a different model_id that does not contain `example` and register you model id at https://forms.gle/kg2Vkpho9BoMXSy57\n"))
}
if(!(model_id %in% registered_model_id$model_id) & !grepl("(example)",model_id)){
  message("Checking if model_id is already used in submissions")
  if(model_id %in% submitted_model_ids$model_id){
    warning(paste0("Your model_id (",model_id,") has not been registered yet but is already used in other submissions.  Please use and register another model_id\n",
                   "   Register at https://forms.gle/kg2Vkpho9BoMXSy57\n",
                  "If you want to submit without registering, include the word 'example' in your model_id.  It will be processed but only retained for 30-days"))
  }else{
    warning(paste0("Your model_id (",model_id,") has not been registered\n",
                   "   Register at https://forms.gle/kg2Vkpho9BoMXSy57\n",
                  "If you want to submit without registering, include the word 'example' in your model_id.  It will be processed but only retained for 30-days"))
  }
  return(NULL)
}

  if(!grepl("(example)",model_id)){
    if(first_submission & model_id %in% registered_model_id$model_id){
      submitted_model_ids <- readr::read_csv("https://renc.osn.xsede.org/bio230121-bucket01/vera4cast/inventory/model_id/model_id-theme-inventory.csv", show_col_types = FALSE)
      if(model_id %in% submitted_model_ids$model_id){
        warning(paste0("Your model_id (",model_id,") is already used in other submitted forecasts. There are two causes for this error: \n
                    - If you have previously submitted a forecast, set the argument `first_submission = FALSE` to remove this error\n
                    - If you have not previously submitted a forecast, this error message means that the model_id has already been registered and used for submissions.  Please register and use another model_id at [https://forms.gle/kg2Vkpho9BoMXSy57](https://forms.gle/kg2Vkpho9BoMXSy57)"))
      }
    }
  }else{
    message("Since `example` is in your model_id, you are submitting an example forecast that will be processed but only retained for 30-days")
  }

  if(!go){

    warning(paste0("forecasts was not in a valid format and was not submitted\n"))
    return(NULL)
  }

  #if(go & ask){
  #
  #  go <- utils::askYesNo(paste0("Forecast file is valid, ready to submit?\n",
  #                               "To remove the need to , set ask = 'TRUE'"))
  #}

  #GENERALIZATION:  Here are specific AWS INFO
  exists <- aws.s3::put_object(file = forecast_file,
                               object = basename(forecast_file),
                               bucket = "vera4cast-submissions",
                               region= s3_region,
                               base_url = s3_endpoint)

  if(exists){
    message("Thank you for submitting! Party Parrot!")
  }else{
    warning("Forecasts was not sucessfully submitted to server. Try again, then contact the Challenge organizers.")
  }
}
