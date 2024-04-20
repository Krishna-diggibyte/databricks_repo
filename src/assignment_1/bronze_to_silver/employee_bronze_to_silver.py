# Databricks notebook source
# DBTITLE 1,calling utils file
# MAGIC %run ../source_to_bronze/utils
# MAGIC

# COMMAND ----------

# DBTITLE 1,callling schema file
# MAGIC %run ./schemas

# COMMAND ----------

# DBTITLE 1,path of csv stored in source_to_bronze
country_path_read='dbfs:/FileStore/assignments/assign_1/source_to_bronze/country_df.csv/part-00000-tid-2429375358653570560-a09a881c-cda1-4428-be38-29a720a67fe8-317-1-c000.csv'

department_path_read='dbfs:/FileStore/assignments/assign_1/source_to_bronze/department_df.csv/part-00000-tid-826393801858667356-6bf8e7e8-db95-4ea3-978c-de0c431bcb79-318-1-c000.csv'

employee_path='dbfs:/FileStore/assignments/assign_1/source_to_bronze/employee_df.csv/part-00000-tid-8958534801557983101-843e525b-d29c-4eeb-8f0d-87dcd37dad59-319-1-c000.csv'



# COMMAND ----------

# DBTITLE 1,read csv by custom schema
country_df=read_schema(country_path_read,country_schema)
department_df=read_schema(department_path_read,department_schema)
employee_df=read_schema(employee_path,employee_schema)


# country_df.display()
# department_df.display()
# employee_df.display()


# COMMAND ----------

# DBTITLE 1,read schema by option methord

country_df=read_schema_options(country_path_read,country_schema)
department_df=read_schema_options(department_path_read,department_schema)
employee_df=read_schema_options(employee_path,employee_schema)

# country_df.display()
# department_df.display()
# employee_df.display()

# COMMAND ----------

# DBTITLE 1,convert camel case to snake case
# 5. convert the Camel case of the columns to the snake case using UDF.
country_df=camel_to_snake(country_df)
department_df=camel_to_snake(department_df)
employee_df=camel_to_snake(employee_df)

# country_df.display()
# department_df.display()
# employee_df.display()


# COMMAND ----------

# DBTITLE 1,add column load_data  having current date
col_name="load_data"
country_df_date=add_load_data(country_df,col_name)
department_df_date=add_load_data(department_df,col_name)
employee_df_date=add_load_data(employee_df,col_name)

# country_df_date.display()
# department_df_date.display()
# employee_df_date.display()

# COMMAND ----------

# DBTITLE 1,make database and use it
# spark.sql('create database employee_info')
# spark.sql('use employee_info')
# spark.catalog.currentDatabase()
# spark.sql('drop database employee_info')

# COMMAND ----------

# DBTITLE 1,make delta table in new database
db_name="employee_info"
table_name="dim_employee"
path=f"dbfs:/FileStore/assignments/assign_1/silver/{db_name}/{table_name}"

write_df(table_name,employee_df,path)
