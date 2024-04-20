# Databricks notebook source
# DBTITLE 1,installing requests module
# pip install requests

# COMMAND ----------

# DBTITLE 1,import files
import requests as re
from pyspark.sql.functions import *
import json
from pyspark.sql.types import *
import logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

# COMMAND ----------

# DBTITLE 1,get data from api
def get_from_api(api):
    krishna=re.get(api)
    return(krishna)

# COMMAND ----------

# DBTITLE 1,creating dataframe from data and custom schema
def create_df(json_krishna,custom_schema):
    df=spark.createDataFrame(data=[json_krishna],schema=custom_schema)
    return df

# COMMAND ----------

# DBTITLE 1,dropping columns
def drop_col(df):
    df1=df.drop('page','per_page','total','total_pages','support')
    return df1

# COMMAND ----------

# DBTITLE 1,explode array data
def explode_data(df):
    new=df.withColumn("all_col",explode('data')).drop(df['data'])
    return new

# COMMAND ----------

# DBTITLE 1,flatten data
def flatten_data(df):
    df1=df.withColumn('id',df.all_col.id).withColumn('email',df.all_col.email).withColumn('first_name',df.all_col.first_name).withColumn('last_name',df.all_col.last_name).withColumn('avatar',df.all_col.avatar).drop(df.all_col)
    return df1


# COMMAND ----------

# DBTITLE 1,split email and use second half as site address
def spilt_email(df):
    df1=df.withColumn("site_address",split(df["email"],"@").getItem(1))
    return df1

# COMMAND ----------

# DBTITLE 1,add current date in df
def add_date(df):
    df1=df.withColumn("load_data",current_date())
    return df1


# COMMAND ----------

# DBTITLE 1,write df to delta file
def write_df(df,path):
    df.write.format('delta').mode("overwrite").save(path)
