# Databricks notebook source
# DBTITLE 1,calling utils file
# MAGIC %run ../source_to_bronze/utils

# COMMAND ----------

# DBTITLE 1,calling schema file
# MAGIC %run ../bronze_to_silver/schemas

# COMMAND ----------

# DBTITLE 1,testing of reading schema function

def test_read_schema():
    expected_result= ['countryCode', 'countryName']
    country_path_read='dbfs:/FileStore/assignments/assign_1/source_to_bronze/country_df.csv/part-00000-tid-2429375358653570560-a09a881c-cda1-4428-be38-29a720a67fe8-317-1-c000.csv'
    country_df=read_schema(country_path_read,country_schema)

    actual_result=country_df.columns
    logging.info(actual_result==expected_result)
test_read_schema()

# COMMAND ----------

# DBTITLE 1,testing of reading schema by option method function
def test_read_schema_options():
    expected_result= ['countryCode', 'countryName']
    country_path_read='dbfs:/FileStore/assignments/assign_1/source_to_bronze/country_df.csv/part-00000-tid-2429375358653570560-a09a881c-cda1-4428-be38-29a720a67fe8-317-1-c000.csv'
    country_df=read_schema_options(country_path_read,country_schema)

    actual_result=country_df.columns
    logging.info(actual_result==expected_result)
test_read_schema_options()

# COMMAND ----------

# DBTITLE 1,testing of function camal to snake case
def test_camel_to_snake():
    expected_result= ['country_code', 'country_name']
    country_path_read='dbfs:/FileStore/assignments/assign_1/source_to_bronze/country_df.csv/part-00000-tid-2429375358653570560-a09a881c-cda1-4428-be38-29a720a67fe8-317-1-c000.csv'
    country_df=read_schema_options(country_path_read,country_schema)
    country_df=camel_to_snake(country_df)

    actual_result=country_df.columns
    logging.info(actual_result==expected_result)
test_camel_to_snake()

# COMMAND ----------

# DBTITLE 1,testing for function add column with current date
def test_add_load_data():
    expected_result= ['country_code', 'country_name', 'load_data'] 
    country_path_read='dbfs:/FileStore/assignments/assign_1/source_to_bronze/country_df.csv/part-00000-tid-2429375358653570560-a09a881c-cda1-4428-be38-29a720a67fe8-317-1-c000.csv'
    country_df=read_schema_options(country_path_read,country_schema)
    country_df=camel_to_snake(country_df)
    col_name="load_data"
    country_df_date=add_load_data(country_df,col_name)

    actual_result=country_df_date.columns
    logging.info(actual_result==expected_result)
test_add_load_data()

# COMMAND ----------

# DBTITLE 1,testing for writing df
def test_write_df():
    expected_result= 'File found'

    db_name="employee_info"
    table_name="dim_employee"
    path=f"dbfs:/FileStore/assignments/assign_1/silver/{db_name}/{table_name}"
    write_df(table_name,employee_df,path)

    try:
        dirs = dbutils.fs.ls (path)
        actual_result='File found'
    except :
        actual_result="The File does not exist"
    logging.info(actual_result==expected_result)
test_write_df()
