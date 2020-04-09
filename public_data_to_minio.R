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
# public announcements ---
public_announcements <- "data/staging/covid_19 announcements.xlsx"
minio_to_file(public_announcements,
              "covid",
              minio_key,
              minio_secret,
              "EDGE",
              minio_filename_override=public_announcements)

covid_general_announcements <- read_excel(public_announcements, sheet = "General announcements")
covid_key_announcements <- read_excel(public_announcements, sheet = "Key announcements")
write_csv(covid_general_announcements, "data/public/covid_general_announcements.csv")
write_csv(covid_key_announcements, "data/public/covid_key_announcements.csv")

# time_series_19-covid-Confirmed ---
# Pull raw data
global_timeseries_confirmed <- read_csv(
  remote_file("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv"))
# Write raw data
write_csv(global_timeseries_confirmed, "data/public/time_series_covid19_confirmed_global.csv")

# r global_ts_sorted_confirmed ---
# Drop unnecessary columns, roll up to country level
global_timeseries_confirmed <- global_timeseries_confirmed %>% 
  dplyr::rename(country = `Country/Region`) %>% 
  select(-Lat, -Long, -`Province/State`)  %>% 
  gather(key = "report_date", value = "confirmed", -country) %>% 
  mutate(report_date = parse_date_time(report_date, orders = c("mdy"))) %>% 
  dplyr::group_by(report_date, country) %>% 
  dplyr::summarise(confirmed = sum(confirmed)) %>%
  dplyr::ungroup() 
# Spread it
global_ts_spread_confirmed <- global_timeseries_confirmed %>% 
  spread(key = "country", value = "confirmed") 
# Remove date, sort cols by most cases, reattach date
global_ts_sorted_confirmed <- global_ts_spread_confirmed %>% 
  select(-report_date) %>% 
  dplyr::select(names(sort(colSums(.)))) %>% 
  mutate(report_date = global_ts_spread_confirmed$report_date) %>% 
  select(report_date, everything())
# Write out csv
write_csv(global_ts_sorted_confirmed, "data/public/global_ts_sorted_confirmed.csv")

# r global_time_since_100 ---
# this rsa max is just a quick workaround to make the SA series appear on the chart.
rsa_max <- global_timeseries_confirmed %>% 
  filter(country == "South Africa") %>% 
  summarise(max(confirmed)) %>% pull()

global_ts_since_100 <- global_timeseries_confirmed %>% 
  mutate(more_than_100 = if_else(confirmed >= min(100, rsa_max), TRUE, FALSE)) %>% 
  filter(more_than_100 == TRUE) %>%
  filter(country != "Cruise Ship",
         country != "China") %>%
  mutate(iter = 1) %>%
  arrange(report_date) %>%
  group_by(country) %>% 
  mutate(days_since_passed_100=cumsum(iter)) %>% 
  ungroup() %>% 
  select(-report_date, -more_than_100, -iter) 

global_ts_since_100 <- global_ts_since_100 %>% 
  spread(key = "country", value = "confirmed")

write_csv(global_ts_since_100, "data/public/global_ts_since_100.csv")

# r time_series_19-covid-Deaths ----------- 
global_timeseries_deaths <- read_csv(
  remote_file("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv"))

write_csv(global_timeseries_deaths, "data/public/time_series_covid19_deaths_global.csv")

# r global_ts_sorted_deaths --------------
global_timeseries_deaths <- global_timeseries_deaths %>% 
  dplyr::rename(country = `Country/Region`) %>% 
  select(-Lat, -Long, -`Province/State`)  %>% 
  gather(key = "report_date", value = "deaths", -country) %>% 
  mutate(report_date = parse_date_time(report_date, orders = c("mdy"))) %>% 
  dplyr::group_by(report_date, country) %>% 
  dplyr::summarise(deaths = sum(deaths)) %>%
  dplyr::ungroup() 
# Spread it
global_ts_spread_deaths <- global_timeseries_deaths %>% 
  spread(key = "country", value = "deaths") 
# Remove date, sort cols by most cases, reattach date
global_ts_sorted_deaths <- global_ts_spread_deaths %>% 
  select(-report_date) %>% 
  dplyr::select(names(sort(colSums(.)))) %>% 
  mutate(report_date = global_ts_spread_deaths$report_date)
write_csv(global_ts_sorted_deaths, "data/public/global_ts_sorted_deaths.csv")

# global_deaths_since_25 -------------------------
rsa_max_deaths <- global_timeseries_deaths %>% 
  filter(country == "South Africa") %>% 
  summarise(max(deaths)) %>% pull()

global_deaths_since_25 <- global_timeseries_deaths %>% 
  mutate(more_than_25 = if_else(deaths >= min(25, rsa_max), TRUE, FALSE)) %>% 
  filter(more_than_25 == TRUE) %>%
  filter(country != "Cruise Ship") %>%
  mutate(iter = 1) %>%
  arrange(report_date) %>%
  group_by(country) %>% 
  mutate(days_since_passed_25=cumsum(iter)) %>% 
  ungroup() %>% 
  select(-report_date, -more_than_25, -iter) 

global_deaths_since_25 <- global_deaths_since_25 %>% 
  spread(key = "country", value = "deaths")

write_csv(global_deaths_since_25, "data/public/global_deaths_since_25.csv")



# r global_latest_stats -------------
global_latest_confirmed <- global_ts_spread_confirmed %>% 
  filter(report_date == max(global_ts_spread_confirmed$report_date)) %>% 
  select(-report_date) %>% 
  t() %>% 
  as.data.frame()

colnames(global_latest_confirmed) <- "confirmed"
global_latest_confirmed$country <- rownames(global_latest_confirmed)

global_latest_deaths <- global_ts_spread_deaths %>% 
  filter(report_date == max(global_ts_spread_deaths$report_date)) %>% 
  select(-report_date) %>% 
  t() %>% 
  as.data.frame()
colnames(global_latest_deaths) <- "deaths"
global_latest_deaths$country <- rownames(global_latest_deaths)

global_latest_data <- left_join(global_latest_confirmed, global_latest_deaths, by = "country")

minio_to_file("data/public/global_country_stats_fixed_names.csv",
              "covid",
              minio_key,
              minio_secret,
              "EDGE",
              minio_filename_override="data/public/global_country_stats_fixed_names.csv")

global_country_stats_fixed_names <- read_csv("data/public/global_country_stats_fixed_names.csv")

global_latest_data <- left_join(global_latest_data, global_country_stats_fixed_names, by=c("country" = "NAME"))

global_latest_data <- drop_na(global_latest_data) 
global_latest_data <- global_latest_data %>% 
  mutate(incidence_per_1m = round((confirmed / population)*10^6, 2), 
         mortality_per_1m = round((deaths / population)*10^6, 2),
         case_fatality_rate_pct = round((deaths / confirmed)*10^2, 2),
         maturity = if_else(confirmed < 101, "0 - 100",
                            if_else(confirmed > 100 & confirmed < 1001, "100 - 1000",
                                    if_else(confirmed > 1000 & confirmed < 10001, "1000 - 10000",
                                            if_else(confirmed > 10000 & confirmed < 100001, "10000 - 100000",
                                                    "100000 +")))))  

write_csv(global_latest_data, "data/public/global_latest_data.csv")

# r covid19za_timeline_provincial_confirmed -------------
rsa_provincial_timeseries_confirmed <- read_csv(
  remote_file("https://raw.githubusercontent.com/dsfsi/covid19za/master/data/covid19za_provincial_cumulative_timeline_confirmed.csv")) %>% 
  mutate(YYYYMMDD = ymd(YYYYMMDD))

write_csv(rsa_provincial_timeseries_confirmed, "data/public/covid19za_provincial_cumulative_timeline_confirmed.csv")

# r covid19za_timeline_confirmed -------------
rsa_timeseries_confirmed <- read_csv(
  remote_file("https://raw.githubusercontent.com/dsfsi/covid19za/master/data/covid19za_timeline_confirmed.csv")) %>% 
  mutate(YYYYMMDD = ymd(YYYYMMDD))

write_csv(rsa_timeseries_confirmed, "data/public/covid19za_timeline_confirmed.csv")


# r covid19za_timeline_testing ----------------
rsa_timeseries_testing <- read_csv(
  remote_file("https://raw.githubusercontent.com/dsfsi/covid19za/master/data/covid19za_timeline_testing.csv")) %>% 
  mutate(YYYYMMDD = ymd(YYYYMMDD))
write_csv(rsa_timeseries_testing, "data/public/covid19za_timeline_testing.csv")


# r covid19za_timeline_deaths -------------------
rsa_timeseries_deaths <- read_csv(
  remote_file("https://raw.githubusercontent.com/dsfsi/covid19za/master/data/covid19za_timeline_deaths.csv")) %>% 
  mutate(YYYYMMDD = ymd(YYYYMMDD))

write_csv(rsa_timeseries_deaths, "data/public/covid19za_timeline_deaths.csv")

#r rsa_provincial_ts_confirmed ------------------
provincial_timeseries_confirmed <- rsa_provincial_timeseries_confirmed %>%
  select(-date)
write_csv(provincial_timeseries_confirmed, "data/public/rsa_provincial_ts_confirmed.csv")

# SEND TO MINIO
public_data_dir <- "data/public/"
for (filename in list.files(public_data_dir)) {
  print(file.path(public_data_dir, filename))
  file_to_minio(file.path(public_data_dir, filename),
                "covid",
                minio_key,
                minio_secret,
                "EDGE",
                filename_prefix_override = public_data_dir)
}  

private_data_dir <- "data/private/"
for (filename in list.files(private_data_dir)) {
  print(file.path(public_data_dir, filename))
  file_to_minio(file.path(public_data_dir, filename),
                "covid",
                minio_key,
                minio_secret,
                "EDGE",
                filename_prefix_override = private_data_dir)
}  

