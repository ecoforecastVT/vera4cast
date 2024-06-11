FROM rocker/geospatial:latest

# Declares build arguments
# ARG NB_USER
# ARG NB_UID

# COPY --chown=${NB_USER} . ${HOME}

RUN install2.r arrow bslib bsicons ggiraph patchwork pak jsonlite reticulate duckdbfs furrr future googlesheets4 here imputeTS tsibble fable RcppRoll RCurl fabletools tidymodels xgboost gdalcubes minioclient gsheet
RUN R -e "devtools::install_github('eco4cast/score4cast')"
RUN sleep 180
RUN R -e "devtools::install_github('cboettig/minioclient')"
RUN sleep 180
RUN R -e "devtools::install_github('LTREB-reservoirs/ver4castHelpers')"
RUN sleep 180
RUN R -e "devtools::install_github('eco4cast/stac4cast')"
RUN sleep 180
RUN R -e "devtools::install_github('cboettig/duckdbfs')"
RUN sleep 180
RUN R -e "devtools::install_github('cboettig/aws.s3')"
RUN sleep 180
RUN R -e "devtools::install_github('FLARE-forecast/RopenMeteo')"
RUN sleep 180
RUN R -e "remotes::install_github('mitchelloharawild/distributional', ref = 'bb0427e')"
RUN sleep 180
RUN R -e "devtools::install_github('eco4cast/gefs4cast')"
#RUN ldd /usr/local/lib/R/site-library/GLM3r/exec/nixglm
