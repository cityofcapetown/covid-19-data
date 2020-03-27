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

save_geojson <- function(sf_frame) {
  savepath <- file.path("data/public", 
                        paste(deparse(substitute(sf_frame)), "geojson", sep = "."))
  if (!("sf" %in% class(sf_frame))) {
    stop("Not a simple feature object!")
  } else {
    st_write(sf_frame, savepath, delete_dsn = TRUE)
    print(paste("Saved to", savepath))
  }
}
# CREATE DIRS =================================================================
dir.create("data/public", recursive = TRUE)
dir.create("data/restricted", recursive = TRUE)

# public announcements ---
public_announcements <- "data/public/covid_19 announcements.xlsx"
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
  filter(country != "China",
         country != "Cruise Ship") %>%
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


# r global_pop_raw ----
if(!file.exists("data/public/global_pop_raw.csv")) {
  #SEX=0 all SEX=1 male SEX=2 female
  global_pop_list <-remote_file("https://api.census.gov/data/timeseries/idb/1year?get=POP,AREA_KM2,NAME&time=2019&AGE=0:120&SEX=0")
  global_pop <- data.frame(matrix(unlist(global_pop_list), 
                                  nrow = length(global_pop_list), 
                                  byrow = T), 
                           stringsAsFactors = FALSE) %>% 
    as_tibble()
  names(global_pop) <- as.character(unlist(global_pop[1,]))
  global_pop <- global_pop[-1,]
  global_pop <- global_pop %>% mutate(POP = as.numeric(POP),
                                      AREA_KM2 = as.numeric(AREA_KM2),
                                      time = as.numeric(time),
                                      AGE = as.numeric(AGE))
  write_csv(global_pop, "data/public/global_pop_raw.csv")
}

# r global_pop_names_fixed.csv ------
global_pop <- read_csv("data/public/global_pop_raw.csv")

global_country_stats <- global_pop %>% dplyr::group_by(NAME) %>% 
  dplyr::summarise(population = sum(POP), area = max(AREA_KM2))

global_country_stats_fixed_names <- global_country_stats %>% 
  mutate(NAME = str_replace(NAME, "Swaziland", "Eswatini"),
         NAME = str_replace(NAME, "United States", "US"),
         NAME = str_replace(NAME, "Taiwan", "Taiwan*"))

write_csv(global_pop, "data/public/global_pop_names_fixed.csv")

# r global_pop_m_raw ----
if(!file.exists("data/public/global_pop_m_raw.csv")) {
  global_pop_m_list <-remote_file("https://api.census.gov/data/timeseries/idb/1year?get=POP,AREA_KM2,NAME&time=2019&AGE=0:120&SEX=0")
  global_pop_m <- data.frame(matrix(unlist(global_pop_m_list), 
                                    nrow = length(global_pop_m_list), 
                                    byrow = T), 
                             stringsAsFactors = FALSE) %>% 
    as_tibble()
  names(global_pop_m) <- as.character(unlist(global_pop_m[1,]))
  global_pop_m <- global_pop_m[-1,]
  global_pop_m <- global_pop_m %>% mutate(POP = as.numeric(POP),
                                          AREA_KM2 = as.numeric(AREA_KM2),
                                          time = as.numeric(time),
                                          AGE = as.numeric(AGE)) 
  write_csv(global_pop_m, "data/public/global_pop_m_raw.csv")
}


# r global_pop_f_raw -----------
if(!file.exists("data/public/global_pop_f_raw.csv")) {
  global_pop_f_list <-remote_file("https://api.census.gov/data/timeseries/idb/1year?get=POP,AREA_KM2,NAME&time=2019&AGE=0:120&SEX=0")
  global_pop_f <- data.frame(matrix(unlist(global_pop_f_list), 
                                    nrow = length(global_pop_f_list), 
                                    byrow = T), 
                             stringsAsFactors = FALSE) %>% 
    as_tibble()
  names(global_pop_f) <- as.character(unlist(global_pop_f[1,]))
  global_pop_f <- global_pop_f[-1,]
  global_pop_f <- global_pop_f %>% mutate(POP = as.numeric(POP),
                                          AREA_KM2 = as.numeric(AREA_KM2),
                                          time = as.numeric(time),
                                          AGE = as.numeric(AGE))
  write_csv(global_pop_f, "data/public/global_pop_f_raw.csv")
}


# r rsa_pop_genders_ages -----------
global_pop_m <- read_csv("data/public/global_pop_m_raw.csv")
global_pop_f <- read_csv("data/public/global_pop_f_raw.csv")

rsa_pop_m <- global_pop_m %>% filter(NAME == "South Africa") %>% dplyr::select(AGE, POP) %>% dplyr::rename(male = POP)

rsa_pop_f <- global_pop_f %>% filter(NAME == "South Africa") %>% dplyr::select(AGE, POP) %>% dplyr::rename(female = POP)

rsa_pop <- left_join(rsa_pop_f, rsa_pop_m, by = "AGE")

write_csv(rsa_pop, "data/public/rsa_pop_genders_ages.csv")


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


# SPATIAL DATA
# Ward density ----------------------
wards <- load_rgdb_table("LDR.SL_CGIS_WARD", minio_key, minio_secret)
wards_2016 <- wards %>% filter(WARD_YEAR == 2016)

wards_2016_polygons <- wards_2016 %>% select(WARD_NAME)

wards_2016_density <- read_csv("data/public/ward_density_2016.csv") %>% 
  mutate(WARD_NAME = as.character(WARD)) %>%
  select(WARD_NAME, `2016_POP`, `2016_POP_DENSITY_KM2`) 

cct_2016_pop_density <- left_join(wards_2016_polygons, wards_2016_density, by = "WARD_NAME") %>% 
  select(WARD_NAME, `2016_POP`, `2016_POP_DENSITY_KM2` ) 

save_geojson(cct_2016_pop_density)

# Health care regions --------------------
health_districts <- load_rgdb_table("LDR.SL_CGIS_CITY_HLTH_RGN", minio_key, minio_secret)
save_geojson(cct_2016_pop_density)

# Health car facilities --------------------
health_care_facilities <- load_rgdb_table("LDR.SL_ENVH_HLTH_CARE_FCLT", minio_key, minio_secret)
save_geojson(health_care_facilities)

informal_taps <- load_rgdb_table("LDR.SL_WTSN_IS_UPDT_TAPS", minio_key, minio_secret)
save_geojson(informal_taps)

informal_toilets <- load_rgdb_table("LDR.SL_WTSN_IS_UPDT_TLTS", minio_key, minio_secret)
save_geojson(informal_toilets)

informal_settlements <- load_rgdb_table("LDR.SL_INF_STLM", minio_key, minio_secret)
save_geojson(informal_settlements)

# SEND TO MINIO
public_data_dir <- "data/public"
for (filename in list.files("data/public")) {
  print(file.path(public_data_dir, filename))
  file_to_minio(file.path(public_data_dir, filename),
                "covid",
                minio_key,
                minio_secret,
                "EDGE",
                filename_prefix_override = "data/public/")
}  

  