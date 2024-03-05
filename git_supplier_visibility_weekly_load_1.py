# Databricks notebook source
# MAGIC %md
# MAGIC The below python script is used to load SV reports xlsx from GCS to ZDL
# MAGIC
# MAGIC Here we first store all excel file into cloud storage bucket and we read this file from that bucket and store it into pandas dataframe 
# MAGIC
# MAGIC Once data is loaded into pandas dataframe we then perform data transformation and then finally load data into stage table 
# MAGIC
# MAGIC What is the difference between stage table and main table is that the stage table is unstandard data i.e all data is in string type and in numeric column for null there is nan 
# MAGIC
# MAGIC So once the data is loaded into stage table we then perform update operation i.e replacing nan for numeric column to null and if there are any unexpected values we can also replace them with any default value
# MAGIC
# MAGIC After all update operation is done we then insert this data into main table 
# MAGIC
# MAGIC The main table will have the correct data type and order of column as per to the requiremnet

# COMMAND ----------

import pandas as pd
from google.cloud import storage
import numpy as np
import pyspark.pandas as ps
from pyspark.sql.functions import expr

# COMMAND ----------

# automation script for week num & year
# import datetime
# #get the current year and week number
# year, week, _day = datetime.date.today().isocalendar()

# bucket_name = "its-managed-dbx-ds-01-d-user-work-area" #bucket name this remains constant

# folder_name = "supplier_visibility_reports_{year}/{year}/WK {week}" 

# COMMAND ----------

bucket_name = "its-managed-dbx-ds-01-d-user-work-area" #bucket name this remains constant
wk = 35
###############################################################################################################################################
# WEEK NUMBER HAS TO BE CHANGED in folder name path for EVERY RUN
###############################################################################################################################################
folder_name = "supplier_visibility_reports_2023/2023/WK 36" #pass the folder name from where you want to read the files - WEEK NUMBER HAS TO BE CHANGED

# COMMAND ----------

# MAGIC %md
# MAGIC list_blobs is used to get all the list of files present in the folder_name

# COMMAND ----------

client = storage.Client()
BUCKET = client.get_bucket(bucket_name)
blobs = BUCKET.list_blobs(prefix=folder_name)
blobs_list = list(blobs)
# del blobs_list[0]
del blobs_list[0]

# COMMAND ----------

for b in blobs_list:
    print(b.name)

# COMMAND ----------

len(blobs_list)

# COMMAND ----------

# MAGIC %md
# MAGIC Here we have two dataframe consolidated_df and log_entry_df 
# MAGIC
# MAGIC consolidated_df will hold data of all the files
# MAGIC
# MAGIC log_entry_df will hold data of each and every file that we read

# COMMAND ----------

consolidated_df = pd.DataFrame()
log_entry_df = pd.DataFrame()
for b in blobs_list: #for loop is used to read all file paths 
    input_path = "gs://its-managed-dbx-ds-01-d-user-work-area/"+b.name
    input_df = pd.read_excel(input_path,sheet_name="PivotTable",skiprows=[0])

    #year_value = ''.join(input_path.split(folder_name+"/")[1].split("/WK")[0]) --changed by MA as this LOC gives the wrong year value
    year_value = input_path.split("/",6)[4]
    week_value = ''.join(input_path.split("WK ")[1].split("/")[0])
    start_date_value = input_df[1].iloc[0]

    input_df.insert(loc=0,column = 'start_date', value = start_date_value)
    input_df.insert(loc=0,column = 'week', value = week_value)
    input_df.insert(loc=0,column = 'year', value = year_value)

    input_df.drop(index=0,inplace=True)
    input_df.drop(input_df.iloc[:, 103:], inplace=True, axis=1)

    consolidated_df = pd.concat([consolidated_df,input_df],axis=0,ignore_index=True) #consolidated df contains consolidated data

    excel_info_df = pd.DataFrame(columns=['year','week','filename','count'],index=[0])

    excel_info_df['year'] = year_value
    excel_info_df['week'] = week_value
    excel_info_df['filename'] = ''.join(input_path.split("Report ")[1].split(".xlsx")[0])
    excel_info_df['count'] = input_df.shape[0]

    log_entry_df = pd.concat([log_entry_df,excel_info_df],axis=0,ignore_index=True) #log entry df contains details related to every file we read
    print("Successfully inserted "+input_path)
print("Successfully inserted all the data!!!")

# COMMAND ----------

input_path

# COMMAND ----------

log_entry_df.head(2)

# COMMAND ----------

consolidated_df.head(2)

# COMMAND ----------

consolidated_df.shape

# COMMAND ----------

# MAGIC %md
# MAGIC Changing datatype of all columns to string so that we can handel unexpected values in later stage

# COMMAND ----------

for col in consolidated_df.columns:
        consolidated_df[col] = consolidated_df[col].astype(str) #changing all cols to object type
        print(consolidated_df[col].dtypes)

# COMMAND ----------

# MAGIC %md
# MAGIC Renaming columns to follow standard column format

# COMMAND ----------

for col in consolidated_df.columns:
    if isinstance(col, int): 
        old_name = str(col)
        new_name = "WK"+old_name #formating wk cols i.e (41 to wk41)
        new_name = new_name.lower()
        print(old_name+":"+new_name)
        consolidated_df.rename(columns = {col:new_name},inplace = True)
        continue
    old_name = str(col)
    new_name = old_name.replace(" ", "_").replace("(","").replace(")","").replace("#","num").replace("-","_to_").replace("/","_or_")
    new_name = new_name.lower()
    print(old_name+":"+new_name) 
    consolidated_df.rename(columns = {old_name:new_name},inplace = True) #renaming columns by removing unexpected string

# COMMAND ----------

cols_in_schema = ['year',
 'week',
 'start_date',
 'site',
 'site_name',
 'part_no',
 'zebra_part_number',
 'manufacturer_part_number',
 'description',
 'tier2_supplier_name',
 'po_extension_at_52_weeks',
 'product_family',
 'sub_family',
 'product_family_list',
 'lead_time',
 'lt_match_sti',
 'raw_cost',
 'safety_stock',
 'lblty',
 'ltb_number',
 'ss_match_sti',
 'psmnum',
 'psm_supplier_name',
 'psm_escalation_level',
 'gsp',
 'wks_1_to_3_short_score',
 'wks_4_to_7_short_score',
 'wks_8_to_9_short_score',
 'wks_10_to_11_short_score',
 'overall_score',
 'supply_risk',
 'lt_in_wks',
 'values_po_coverage_within_lt',
 'shortage_at_lt_y_or_n',
 'po_coverage_at_lt_y_or_n',
 'po_coverage_at_52_wks',
 'coverage_52_wks_y_or_n',
 'shortage_on_wknum',
 'shortage_weekly_bucketnum',
 'risk_at_lt',
 'oh',
 'soi',
 'zoi',
 'hub',
 'total_open_po_qty',
 '52_wks_demand',
 'values',
 'num',
 'past_due',
 'wk1',
 'wk2',
 'wk3',
 'wk4',
 'wk5',
 'wk6',
 'wk7',
 'wk8',
 'wk9',
 'wk10',
 'wk11',
 'wk12',
 'wk13',
 'wk14',
 'wk15',
 'wk16',
 'wk17',
 'wk18',
 'wk19',
 'wk20',
 'wk21',
 'wk22',
 'wk23',
 'wk24',
 'wk25',
 'wk26',
 'wk27',
 'wk28',
 'wk29',
 'wk30',
 'wk31',
 'wk32',
 'wk33',
 'wk34',
 'wk35',
 'wk36',
 'wk37',
 'wk38',
 'wk39',
 'wk40',
 'wk41',
 'wk42',
 'wk43',
 'wk44',
 'wk45',
 'wk46',
 'wk47',
 'wk48',
 'wk49',
 'wk50',
 'wk51',
 'wk52',
 'wk53',
 'wk54']
consolidated_df['wk54']= np.nan
consolidated_df['wk54'] = consolidated_df['wk54'].astype('str')
consolidated_df = consolidated_df[cols_in_schema]

# COMMAND ----------

# MAGIC %md
# MAGIC Converting pandas df to spark df and then append into stage table

# COMMAND ----------

consolidated_spark_df = spark.createDataFrame(consolidated_df) #converting df to spark df

# COMMAND ----------

# automation script for week num & year

# consolidated_spark_df.write.mode("append").format("delta").saveAsTable("pbi_tables_vw.supplier_visibility_reports_{year}_stage") #appending data into stage table

# COMMAND ----------

consolidated_spark_df.write.mode("append").format("delta").saveAsTable("pbi_tables_vw.supplier_visibility_reports_2023_stage") #appending data into stage table

# COMMAND ----------

# MAGIC %md
# MAGIC Converting pandas df to spark df and then append into log table

# COMMAND ----------

log_entry_spark_df = spark.createDataFrame(log_entry_df)

# COMMAND ----------

# automation script for week num & year
# log_entry_spark_df.write.mode("append").format("delta").saveAsTable("pbi_tables_vw.log_entry_supplier_visibility_reports_{year}") #appending data into log table

# COMMAND ----------

log_entry_spark_df.write.mode("append").format("delta").saveAsTable("pbi_tables_vw.log_entry_supplier_visibility_reports_2023") #appending data into log table
