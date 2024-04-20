# Databricks notebook source
# DBTITLE 1,call utils
# MAGIC %run ./utils

# COMMAND ----------

# DBTITLE 1,call schema
# MAGIC %run ./schema

# COMMAND ----------

# DBTITLE 1,import unit test
import unittest

# COMMAND ----------

# DBTITLE 1,testing loading data from api
def test_get_from_api():
        expected_result= '<Response [200]>'
        api= 'https://reqres.in/api/users?page=2'
        actual_result=str(get_from_api(api))
        logging.info(actual_result==expected_result)
test_get_from_api()

# COMMAND ----------

# DBTITLE 1,testing create dataframe
def test_create_df():
    expected_result= ['page', 'per_page', 'total', 'total_pages', 'data', 'support']
    api= 'https://reqres.in/api/users?page=2'
    krishna=get_from_api(api)
    json_krishna=krishna.json()
    df=create_df(json_krishna,custom_schema)
    actual_result=df.columns
    logging.info(actual_result==expected_result)

test_create_df()

# COMMAND ----------

# DBTITLE 1,testing droping column
def test_drop_col():
    expected_result=['data']
    api= 'https://reqres.in/api/users?page=2'
    krishna=get_from_api(api)
    json_krishna=krishna.json()
    df=create_df(json_krishna,custom_schema)
    df1=drop_col(df)
    actual_result=df1.columns
    logging.info(actual_result==expected_result)
test_drop_col()

# COMMAND ----------

# DBTITLE 1,testing explode function
def test_explode_data():
    expected_result=6
    api= 'https://reqres.in/api/users?page=2'
    krishna=get_from_api(api)
    json_krishna=krishna.json()
    df=create_df(json_krishna,custom_schema)
    df1=drop_col(df)
    new=explode_data(df1)

    actual_result=new.count()
    logging.info(actual_result==expected_result)
test_explode_data()


# COMMAND ----------

# DBTITLE 1,testing flatten function
def test_flatten_data():
    expected_result=['id', 'email', 'first_name', 'last_name', 'avatar']
    api= 'https://reqres.in/api/users?page=2'
    krishna=get_from_api(api)
    json_krishna=krishna.json()
    df=create_df(json_krishna,custom_schema)
    df1=drop_col(df)
    new=explode_data(df1)
    flat_df=flatten_data(new)

    actual_result=flat_df.columns
    logging.info(actual_result==expected_result)
test_flatten_data()

# COMMAND ----------

# DBTITLE 1,testing split function
def test_spilt_email():
    expected_result=['id', 'email', 'first_name', 'last_name', 'avatar', 'site_address']
    api= 'https://reqres.in/api/users?page=2'
    krishna=get_from_api(api)
    json_krishna=krishna.json()
    df=create_df(json_krishna,custom_schema)
    df1=drop_col(df)
    new=explode_data(df1)
    flat_df=flatten_data(new)
    new_flat_df=spilt_email(flat_df)

    actual_result=new_flat_df.columns
    logging.info(actual_result==expected_result)
test_spilt_email()

# COMMAND ----------

# DBTITLE 1,testing adding date column
def test_add_date():
    expected_result=['id', 'email', 'first_name', 'last_name', 'avatar', 'site_address', 'load_data']
    api= 'https://reqres.in/api/users?page=2'
    krishna=get_from_api(api)
    json_krishna=krishna.json()
    df=create_df(json_krishna,custom_schema)
    df1=drop_col(df)
    new=explode_data(df1)
    flat_df=flatten_data(new)
    new_flat_df=spilt_email(flat_df)
    new_df_date=add_date(new_flat_df)

    actual_result=new_df_date.columns
    logging.info(actual_result==expected_result)
test_add_date()

# COMMAND ----------

# DBTITLE 1,testing write dataframe
def test_write_df():
    expected_result='File found'

    api= 'https://reqres.in/api/users?page=2'
    krishna=get_from_api(api)
    json_krishna=krishna.json()
    df=create_df(json_krishna,custom_schema)
    df1=drop_col(df)
    new=explode_data(df1)
    flat_df=flatten_data(new)
    new_flat_df=spilt_email(flat_df)
    new_df_date=add_date(new_flat_df)

    db_name = 'site_info'
    table_name = 'person_info'
    path=f"dbfs:/FileStore/assignments/assign_2/{db_name}/{table_name}"
    write_df(new_df_date,path)

    try:
        dirs = dbutils.fs.ls (path)
        actual_result='File found'
    except :
        actual_result="The File does not exist"
    logging.info(actual_result==expected_result)
test_write_df()
