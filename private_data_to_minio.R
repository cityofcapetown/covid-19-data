# BOILERPLATE =================================================================================================
rm(list=ls())
options(scipen=999)
args = commandArgs(trailingOnly=TRUE)

# SETENV FOR DAG COMPATIBILITY ===================================================================================
if (Sys.getenv("DB_UTILS_DIR") == "") {
  Sys.setenv("DB_UTILS_DIR" = "~/db-utils")
}
if (Sys.getenv("SECRETS_FILE") == "") {
  Sys.setenv("SECRETS_FILE" = "~/secrets.json")
}

# LOAD LIBRARIES ============================================================================
source(file.path(Sys.getenv("DB_UTILS_DIR"), "/R/db-utils.R"), chdir = TRUE)
library(tidyverse)
library(readr)           
library(lubridate)
library(jsonlite)
library(httr)
library(purrr)
library(readxl)
library(arrow)
library(sf)

# LOAD SECRETS ==========================================================================
# Credentials
secrets <- fromJSON(Sys.getenv("SECRETS_FILE"))
# Load minio credentials
minio_key <- secrets$minio$edge$access
minio_secret <- secrets$minio$edge$secret
data_classification = "EDGE"
filename_prefix_override = NA
minio_url_override = NA

# Deal with proxy
httr::set_config(config(ssl_verifypeer = 0L)) # because mportal cert invalid
options(scipen=999) # disable scientific notation
safe_GET <- purrr::safely(httr::GET)

# FUNCTIONS =================================================================
# Function to pull files through firewall
remote_file <- function(url) {
  request_object <- safe_GET(url, 
                             use_proxy("internet05.capetown.gov.za", 
                                       port = 8080, 
                                       username = secrets$proxy$username, 
                                       password = secrets$proxy$password,
                                       auth = "basic"),
                             progress())
  
  content(request_object$result)
}

load_rgdb_table <- function(table_name, minio_key, minio_secret) {
  temp_dir <- tempdir()
  cat("Loading", table_name, "from rgdb bucket and reading into memory \n")
  filename = file.path(temp_dir, paste(table_name, ".parquet", sep = ""))
  if (!file.exists(filename)) {
    minio_to_file(filename,
                  minio_key=minio_key,
                  minio_secret=minio_secret,
                  minio_bucket="rgdb",
                  data_classification= "EDGE")
  }
  wkt_df <- read_parquet(filename)
  cat("Converting", table_name, "data frame into spatial feature \n")
  geo_layer <- st_as_sf(wkt_df, wkt = "EWKT")
  names(st_geometry(geo_layer)) <- NULL
  return(geo_layer)
}

# CREATE DIRS =================================================================
unlink("data/public", recursive= T)
unlink("data/private", recursive = T)
unlink("data/staging", recursive = T)
dir.create("data/public", recursive = TRUE)
dir.create("data/private", recursive = TRUE)
dir.create("data/staging", recursive = T)

# PROCESS DATA

# WC_case_data ---
wc_case_data <- "data/staging/Covid-19 Anonymised line list.csv"
minio_to_file(wc_case_data,
              "covid",
              minio_key,
              minio_secret,
              "EDGE",
              minio_filename_override=wc_case_data)

wc_all_cases <- read_csv(wc_case_data)
write_csv(wc_all_cases, "data/private/wc_all_cases.csv")

ct_all_cases <- wc_all_cases %>% filter(district == "City of Cape Town")
write_csv(ct_all_cases, "data/private/ct_all_cases.csv")

# WC_model_data ---------------------------
wc_model_data_new <- "data/staging/wc_covid_scenarios.xlsx"
minio_to_file(wc_model_data_new,
              "covid",
              minio_key,
              minio_secret,
              "EDGE",
              minio_filename_override=wc_model_data_new)
wc_model_data_new <- read_xlsx(wc_model_data_new)
wc_model_data_new <- wc_model_data_new %>% 
  mutate(key = paste(TimeInterval, 
                     NewInfections, 
                     NewGeneralAdmissions, 
                     NewICUAdmissions, 
                     GeneralBedNeed, 
                     ICUBedNeed, 
                     NewDeaths, 
                     Scenario, sep = "|")) 

wc_model_data_old <- "data/private/wc_model_data.csv"
minio_to_file(wc_model_data_old,
              "covid",
              minio_key,
              minio_secret,
              "EDGE",
              minio_filename_override=wc_model_data_old)
wc_model_data_old <- read_csv(wc_model_data_old)
wc_model_data_old <- wc_model_data_old %>% 
  mutate(key = paste(TimeInterval, 
                     NewInfections, 
                     NewGeneralAdmissions, 
                     NewICUAdmissions, 
                     GeneralBedNeed, 
                     ICUBedNeed, 
                     NewDeaths, 
                     Scenario, sep = "|")) 

wc_model_data_new <- wc_model_data_new %>% filter(!(key %in% wc_model_data_old$key))
if (nrow(wc_model_data_new) != 0 ) {
  wc_model_data_new <- wc_model_data_new %>% mutate(ForecastDate = Sys.time()) %>% dplyr::select(ForecastDate, everything(), -key)
  wc_model_data_old <- wc_model_data_old %>%dplyr::select(-key)
  
  wc_model_data <- bind_rows(wc_model_data_old, wc_model_data_new)
  write_csv(wc_model_data, "data/private/wc_model_data.csv")
}


# SEND DATA TO MINIO ==================================

private_data_dir <- "data/private/"
for (filename in list.files(private_data_dir)) {
  print(file.path(private_data_dir, filename))
  file_to_minio(file.path(private_data_dir, filename),
                "covid",
                minio_key,
                minio_secret,
                "EDGE",
                filename_prefix_override = private_data_dir)
}  
