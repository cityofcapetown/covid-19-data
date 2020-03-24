# covid-19-data

Listing:
* `public_data_to_minio.R` - pulls data from various public datasets, pushes into minio
* `income_finance_data_to_minio.py` - pulls "TOTAL ISU" data file from OPM Data email account, pushes into minio
* `income_finance_data_munge.py` - pulls "TOTAL_ISU" data file from minio, cleans up, and puts back in as "income_totals.csv"
* `media_data_to_mino.py` - pulls all media data from Brandseye API, and puts it into Minio as "media_complete.csv". Since it's trivial, also does a filter, and produces "media_filtered.csv".