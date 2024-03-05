# Databricks notebook source
# MAGIC %md
# MAGIC The below python script is used to fetch unique pair of year, week and sit_name from main table and then use this site_name to create matrix for comparing availablility of that record in main table w.r.t that year and week
# MAGIC
# MAGIC A matrix is a dataframe which holds value in such a way that for every unqiue supplier_name it will have 52 records which implies 52 weeks

# COMMAND ----------

import pandas as pd
import numpy as np

# COMMAND ----------

query="SELECT distinct year,week,site_name from pbi_tables_vw.supplier_visibility_2021_reports" #fecthing all unique site_name with year and week data
log_entry_df=spark.sql(query) #storing query result into spark df
log_entry_df=log_entry_df.toPandas()

# COMMAND ----------

for col in log_entry_df.columns:
        log_entry_df[col] = log_entry_df[col].astype(str) #changing all cols to object type
        print(log_entry_df[col].dtypes)

# COMMAND ----------

log_entry_df.rename(columns = {'site_name':'supplier_name'}, inplace = True) 

# COMMAND ----------

my_supplier_list = []
for i in range(len(log_entry_df)):
    supplier_name = log_entry_df.loc[i, "supplier_name"] #fetching list of all supplier name
    my_supplier_list.append(supplier_name)

# COMMAND ----------

my_supplier_list

# COMMAND ----------

my_unique_supplier_list = list(set(my_supplier_list)) #list of all unique supplier_name

# COMMAND ----------

for i in my_unique_supplier_list:
    print(i)

# COMMAND ----------

#creating a matrix df in such a way that for each supplier we create year and w.r.t that year we have all week rows 
matrix_status_info_df = pd.DataFrame()
for i in range(len(my_unique_supplier_list)):
    for j in range(1,53):
        status_info_df = pd.DataFrame(columns=['supplier_name','year','week'],index=[0])

        status_info_df['supplier_name'] = my_unique_supplier_list[i]
        status_info_df['year'] = "2021"
        if j >=1 and j<=9:
            status_info_df['week'] = "0"+str(j)
        else:
            status_info_df['week'] = str(j)

        matrix_status_info_df = pd.concat([matrix_status_info_df,status_info_df],axis=0,ignore_index=True)
print("Succsessfully created the matrix dataframe")

# COMMAND ----------

#once we have matrix df we compare it with log_entry_df for existence of that records and pass that value in new col i.e status 
consolidated_status_info_df = pd.merge(matrix_status_info_df,log_entry_df, on =['year','week','supplier_name'],how='left',indicator='status')

# COMMAND ----------

consolidated_status_info_df['status'] = np.where(consolidated_status_info_df.status == 'both', 'Available', 'Missing') #replacing value 

# COMMAND ----------

consolidated_status_info_df.head(2)

# COMMAND ----------

consolidated_status_info_spark_df = spark.createDataFrame(consolidated_status_info_df)

# COMMAND ----------

consolidated_status_info_spark_df.write.mode("append").format("delta").saveAsTable("pbi_tables_vw.status_supplier_visibility_2021_reports")

# COMMAND ----------


