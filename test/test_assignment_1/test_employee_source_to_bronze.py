# Databricks notebook source
# DBTITLE 1,calling utils
# MAGIC %run ../source_to_bronze/utils

# COMMAND ----------

# DBTITLE 1,testing for reading csv file
def test_read_csv_data():
    expected_result=6
    country_path='dbfs:/FileStore/assignments/assign_1/resource/Country_Q1.csv'
    country_df=read_csv_data(country_path)

    actual_result=country_df.count()
    logging.info(actual_result==expected_result)
test_read_csv_data()


# COMMAND ----------

# DBTITLE 1,testing for wrting csv files
def test_write_csv_file():
    expected_result='File found'
    country_path='dbfs:/FileStore/assignments/assign_1/resource/Country_Q1.csv'
    country_df=read_csv_data(country_path)
    
    save_path='/FileStore/assignments/assign_1/source_to_bronze/country_df.csv'
    write_csv_file(country_df,save_path)
    try:
        dirs = dbutils.fs.ls(save_path)
        actual_result='File found'
    except :
        actual_result="The File does not exist"
    logging.info(actual_result==expected_result)
test_write_csv_file()

