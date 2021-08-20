@echo off

pip install pandas
pip install google-cloud-bigquery 
pip install google-cloud-bigquery-storage
pip install --upgrade google-cloud-bigquery[bqstorage,pandas]
pip install openpyxl

pause
exit