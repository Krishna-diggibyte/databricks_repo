# Databricks notebook source
# DBTITLE 1,call utils file
# MAGIC %run ../source_to_bronze/utils
# MAGIC

# COMMAND ----------

# DBTITLE 1,call schema file
# MAGIC %run ../bronze_to_silver/schemas

# COMMAND ----------

# DBTITLE 1,reading delta table
db_name="employee_info"
table_name="dim_employee"
path=f"dbfs:/FileStore/assignments/assign_1/silver/{db_name}/{table_name}"
delta_df = read_delta(path)
# display(delta_df)

# COMMAND ----------

# DBTITLE 1,·display salary of each department in descending order.
desc_sal=find_sal_dep(delta_df)
# display(desc_sal)

# COMMAND ----------

# DBTITLE 1,number of employees in each department located in each country
no_of_emp_dept=delta_df.groupBy("country").count()
# display(no_of_emp_dept)

# COMMAND ----------

# DBTITLE 1,path of csv stored in source_to_bronze
country_path_read='dbfs:/FileStore/assignments/assign_1/source_to_bronze/country_df.csv/part-00000-tid-2429375358653570560-a09a881c-cda1-4428-be38-29a720a67fe8-317-1-c000.csv'

department_path_read='dbfs:/FileStore/assignments/assign_1/source_to_bronze/department_df.csv/part-00000-tid-826393801858667356-6bf8e7e8-db95-4ea3-978c-de0c431bcb79-318-1-c000.csv'

employee_path='dbfs:/FileStore/assignments/assign_1/source_to_bronze/employee_df.csv/part-00000-tid-8958534801557983101-843e525b-d29c-4eeb-8f0d-87dcd37dad59-319-1-c000.csv'


# COMMAND ----------

# DBTITLE 1,creating df by reading csv
country_df=read_schema(country_path_read,country_schema)
department_df=read_schema(department_path_read,department_schema)
employee_df=read_schema(employee_path,employee_schema)


# country_df.display()
# department_df.display()
# employee_df.display()

# COMMAND ----------

# DBTITLE 1,List the department names along with their corresponding country names.
df1=df_joins(employee_df,country_df,department_df)
dep_contry_df=df1.orderBy('departmentName')

# display(dep_contry_df)

# COMMAND ----------

# DBTITLE 1,average age of employees in each department
avg_age_df=find_avg(delta_df,Window)

# display(avg_age_df)

# COMMAND ----------

# DBTITLE 1,Add the at_load_date column to data frames
col_name="at_load_data"
country_df_date=add_load_data(country_df,col_name)
department_df_date=add_load_data(department_df,col_name)
employee_df_date=add_load_data(employee_df,col_name)

# country_df_date.display()
# department_df_date.display()
# employee_df_date.display()

# COMMAND ----------

# DBTITLE 1,Write employee df to dbfs
folder='employee'
table_name='fact_employee'
df_name=employee_df_date

path=f"dbfs:/FileStore/assignments/assign_1/gold/{folder}/{table_name}"

write_df_gold(df_name,path)

# display(spark.read.format('delta').load(path)) 

# COMMAND ----------

# DBTITLE 1,Write country df to dbfs
folder='country'
table_name='fact_country'
df_name=country_df_date

path=f"dbfs:/FileStore/assignments/assign_1/gold/{folder}/{table_name}"

write_df_gold(df_name,path)
# display(spark.read.format('delta').load(path)) 

# COMMAND ----------

# DBTITLE 1,Write department df to dbfs
folder='department'
table_name='fact_department'
df_name=department_df_date

path=f"dbfs:/FileStore/assignments/assign_1/gold/{folder}/{table_name}"

write_df_gold(df_name,path)
# display(spark.read.format('delta').load(path)) 
