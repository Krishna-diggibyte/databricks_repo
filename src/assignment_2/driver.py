# Databricks notebook source
# DBTITLE 1,calling utils
# MAGIC %run ./utils

# COMMAND ----------

# DBTITLE 1,calling schema
# MAGIC %run  ./schema

# COMMAND ----------

# DBTITLE 1,hitting api with get
api= 'https://reqres.in/api/users?page=2'
krishna=get_from_api(api)
json_krishna=krishna.json()

# COMMAND ----------

# DBTITLE 1,defining custom schema and making df


df=create_df(json_krishna,custom_schema)
# display(df)

# COMMAND ----------

# DBTITLE 1,droping columns
df1=drop_col(df)
# df1.display()

# COMMAND ----------

# DBTITLE 1,explode data from dataframe
new=explode_data(df1)
# new.display()
# new.printSchema()

# COMMAND ----------

# DBTITLE 1,flatten api data
flat_df=flatten_data(new)
# display(flat_df)

# COMMAND ----------

# DBTITLE 1,split email and extract site address
new_flat_df=spilt_email(flat_df)
# display(new_flat_df)

# COMMAND ----------

# DBTITLE 1,add curunt date in table
new_df_date=add_date(new_flat_df)
# display(new_df_date)

# COMMAND ----------

# DBTITLE 1,write df to delta table
db_name = 'site_info'
table_name = 'person_info'
path=f"dbfs:/FileStore/assignments/assign_2/{db_name}/{table_name}"

write_df(new_df_date,path)

# display(spark.read.format('delta').load(path))
