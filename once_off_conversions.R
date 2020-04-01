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

save_geojson <- function(sf_frame, savedir) {
  savepath <- file.path(savedir, 
                        paste(deparse(substitute(sf_frame)), "geojson", sep = "."))
  if (!("sf" %in% class(sf_frame))) {
    stop("Not a simple feature object!")
  } else {
    st_write(sf_frame, savepath, delete_dsn = TRUE)
    print(paste("Saved to", savepath))
  }
}


# CREATE DIRS =================================================================
unlink("data/public", recursive= T)
unlink("data/restricted", recursive = T)
unlink("data/staging", recursive = T)
dir.create("data/public", recursive = TRUE)
dir.create("data/restricted", recursive = TRUE)
dir.create("data/staging", recursive = T)

# PROCESS =====================================================================
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
write_csv(global_country_stats_fixed_names, "data/public/global_country_stats_fixed_names.csv")

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

# SPATIAL CITY DATA
# Ward density ----------------------
wards_2016_density <- "data/public/ward_density_2016.csv"
minio_to_file(wards_2016_density,
              "covid",
              minio_key,
              minio_secret,
              "EDGE",
              minio_filename_override=wards_2016_density)


wards <- load_rgdb_table("LDR.SL_CGIS_WARD", minio_key, minio_secret)
wards_2016 <- wards %>% filter(WARD_YEAR == 2016)

wards_2016_polygons <- wards_2016 %>% select(WARD_NAME)

wards_2016_density <- read_csv("data/public/ward_density_2016.csv") %>% 
  mutate(WARD_NAME = as.character(WARD)) %>%
  select(WARD_NAME, `2016_POP`, `2016_POP_DENSITY_KM2`) 

cct_2016_pop_density <- left_join(wards_2016_polygons, wards_2016_density, by = "WARD_NAME") %>% 
  select(WARD_NAME, `2016_POP`, `2016_POP_DENSITY_KM2` ) 

save_geojson(cct_2016_pop_density, "data/public")

# Health care regions --------------------
health_districts <- load_rgdb_table("LDR.SL_CGIS_CITY_HLTH_RGN", minio_key, minio_secret)
save_geojson(health_districts, "data/public")

# Health car facilities --------------------
health_care_facilities <- load_rgdb_table("LDR.SL_ENVH_HLTH_CARE_FCLT", minio_key, minio_secret)
save_geojson(health_care_facilities, "data/public")

# Informal taps ------------------------
informal_taps <- load_rgdb_table("LDR.SL_WTSN_IS_UPDT_TAPS", minio_key, minio_secret)
save_geojson(informal_taps, "data/public")

# Informal toilets -------------------
informal_toilets <- load_rgdb_table("LDR.SL_WTSN_IS_UPDT_TLTS", minio_key, minio_secret)
save_geojson(informal_toilets, "data/public")

# Informal settlements ------------
informal_settlements <- load_rgdb_table("LDR.SL_INF_STLM", minio_key, minio_secret)
save_geojson(informal_settlements, "data/public")

# PGWC Large Files Server
staging_root <- "data/staging" 
filedir <- "City"
filename <- file.path(staging_root, paste(filedir,"zip", sep = "."))
minio_to_file(filename,
              "covid",
              minio_key,
              minio_secret,
              "EDGE",
              minio_filename_override=filename)
unzip(filename, exdir = staging_root)
pgwc_cct_polygons <- read_sf(file.path(staging_root, filedir))
save_geojson(pgwc_cct_polygons, "data/public")

filedir <- "Province"
filename <- file.path(staging_root, paste(filedir,"zip", sep = "."))
minio_to_file(filename,
              "covid",
              minio_key,
              minio_secret,
              "EDGE",
              minio_filename_override=filename)
unzip(filename, exdir = staging_root)
pgwc_wc_province_polygons <- read_sf(file.path(staging_root, filedir))
save_geojson(pgwc_wc_province_polygons, "data/public")

# CCT Hex level 7 ------------------------

minio_to_file("data/staging/city-hex-polygons-7.geojson",
              "city-hex-polygons",
              minio_key,
              minio_secret,
              "EDGE",
              minio_filename_override="city-hex-polygons-7.geojson")
cct_hex_polygons_7 <- read_sf("data/staging/city-hex-polygons-7.geojson")
save_geojson(cct_hex_polygons_7, "data/public")

# Resilience 

staging_root <- "data/staging" 
filedir <- "Climate Risk Study - Resilience"
filename <- file.path(staging_root, paste(filedir,"zip", sep = "."))
minio_to_file(filename,
              "covid",
              minio_key,
              minio_secret,
              "EDGE",
              minio_filename_override=filename)

unzip(filename)

pgwc_cct_polygons <- read_sf(file.path(staging_root, filedir))
save_geojson(pgwc_cct_polygons, "data/public")



# SEND TO MINIO ====================================
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

