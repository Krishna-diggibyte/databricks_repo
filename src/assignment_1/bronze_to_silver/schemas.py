# Databricks notebook source
# DBTITLE 1,Schemas for country, department and employee df 
country_schema=StructType([
    StructField("countryCode",StringType(),True),
    StructField("countryName",StringType(),True)
])
department_schema=StructType([
    StructField("departmentId",StringType(),True),
    StructField("departmentName",StringType(),True)
])
employee_schema=StructType([
    StructField("employeeId",IntegerType(),True),
    StructField("employeeName",StringType(),True),
    StructField("department",StringType(),True), 
    StructField("country",StringType(),True),
    StructField("salary",IntegerType(),True),
    StructField("age",IntegerType(),True)
])

