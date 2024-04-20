# Databricks notebook source
# DBTITLE 1,schema for json
# Define the schema for the data field
data_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("email", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("avatar", StringType(), True)
])
 
 
# Define the main schema
custom_schema = StructType([
    StructField("page", IntegerType(), True),
    StructField("per_page", IntegerType(), True),
    StructField("total", IntegerType(), True),
    StructField("total_pages", IntegerType(), True),
    StructField("data", ArrayType(data_schema), True),
    StructField("support", MapType(StringType(), StringType()), True)
])
