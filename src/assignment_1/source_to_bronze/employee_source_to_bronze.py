# Databricks notebook source
# DBTITLE 1,calling utils
# MAGIC %run ./utils

# COMMAND ----------

# DBTITLE 1,read all csv from resource
# read all csv

country_path='dbfs:/FileStore/assignments/assign_1/resource/Country_Q1.csv'
department_path='/FileStore/assignments/assign_1/resource/Department_Q1.csv'
employee_path='/FileStore/assignments/assign_1/resource/Employee_Q1.csv'

country_df=read_csv_data(country_path)
department_df=read_csv_data(department_path)
employee_df=read_csv_data(employee_path)

# country_df.display()
# department_df.display()
# employee_df.display()


# COMMAND ----------

# DBTITLE 1,write csv file to source_to_bronze
# write all csv files

write_csv_file(country_df,'/FileStore/assignments/assign_1/source_to_bronze/country_df.csv')
write_csv_file(department_df,'/FileStore/assignments/assign_1/source_to_bronze/department_df.csv')
write_csv_file(employee_df,'/FileStore/assignments/assign_1/source_to_bronze/employee_df.csv')
