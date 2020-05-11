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
library(jsonlite)
library(readxl)
library(dplyr)
library(tidyr)
library(lubridate)
library(readr)

# LOAD SECRETS ==========================================================================
# Credentials
secrets <- fromJSON(Sys.getenv("SECRETS_FILE"))
# Load minio credentials
minio_key <- secrets$minio$edge$access
minio_secret <- secrets$minio$edge$secret
data_classification = "EDGE"
filename_prefix_override = NA
minio_url_override = NA

# CODE ======================================================================================

# CREATE DIRS =================================================================
unlink("data/staging", recursive = T)
dir.create("data/staging", recursive = T)

# PROCESS DATA ====================================================================

billing_data <- "data/staging/MCSH-Billing_Invoicing.xlsx"
minio_to_file(billing_data,
              "covid",
              minio_key,
              minio_secret,
              "EDGE",
              minio_filename_override=billing_data)


billing_data <- read_excel(billing_data, sheet = "2020 Billing totals", range = "A3:AK26", col_names = F)

billing_data_wip <- billing_data %>% 
  t() %>% 
  as_tibble(.name_repair = "unique")  %>% 
  select(-`...3`) %>% 
  fill(`...1`, .direction = "down")

colnames(billing_data_wip) <- billing_data_wip[1,]
colnames(billing_data_wip)[1] <- "Month"
colnames(billing_data_wip)[2] <- "Metric"
billing_data_wip <- billing_data_wip[-1,]
         
billing_data_wip <- billing_data_wip %>% 
  gather(key = "Portion", value = "Amount", -Month, -Metric)

billing_data_tidy <- billing_data_wip %>% 
  mutate(Amount = as.numeric(Amount), 
         Portion = substr(Portion, start = 2, stop = 3)) %>%
  mutate(Date = dmy(paste(Portion, Month))) %>%
  select(-Month, -Portion) %>% 
  mutate(Timestamp = round((as.numeric(as.POSIXct(Sys.time()))*10^3),0)) %>% 
  select(Timestamp, Date, Metric, Amount) 


# Read in existing data
private_dir <- "data/private"
billing_totals_file <- file.path(private_dir, "billing_totals_versioned.csv")

minio_to_file(billing_totals_file, 
              minio_bucket = "covid",
              minio_key = minio_key,
              minio_secret = minio_secret,
              data_classification = "EDGE",
              minio_filename_override = billing_totals_file)

billing_totals <- read_csv(billing_totals_file)

# Join to existing data and update any new values
# Versioned values
billing_totals_versioned <- bind_rows(billing_totals, billing_data_tidy) %>% 
  arrange(desc(Timestamp, Date, Metric)) %>% 
   distinct(Date, Metric, Amount, .keep_all = TRUE)

# Latest values
billing_totals_latest <- bind_rows(billing_totals, billing_data_tidy) %>% 
  arrange(desc(Timestamp, Date, Metric)) %>% 
  distinct(Date, Metric, .keep_all = TRUE)

write_csv(billing_totals_versioned, "data/private/billing_totals_versioned.csv")
write_csv(billing_totals_latest, "data/private/billing_totals.csv")

file_to_minio("data/private/billing_totals_versioned.csv",
              "covid",
              minio_key,
              minio_secret,
              "EDGE",
              filename_prefix_override = "data/private/")

file_to_minio("data/private/billing_totals.csv",
              "covid",
              minio_key,
              minio_secret,
              "EDGE",
              filename_prefix_override = "data/private/")
