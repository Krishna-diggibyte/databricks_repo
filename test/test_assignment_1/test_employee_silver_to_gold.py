# Databricks notebook source
# DBTITLE 1,calling utils file
# MAGIC %run ../source_to_bronze/utils

# COMMAND ----------

# DBTITLE 1,calling schema file
# MAGIC %run ../bronze_to_silver/schemas

# COMMAND ----------

# DBTITLE 1,testing reading of delta table
def test_read_delta():
    expected_result= ['employee_id', 'employee_name', 'department', 'country', 'salary', 'age']
    db_name="employee_info"
    table_name="dim_employee"
    path=f"dbfs:/FileStore/assignments/assign_1/silver/{db_name}/{table_name}"
    delta_df = read_delta(path)

    actual_result= delta_df.columns
    logging.info(actual_result==expected_result)
test_read_delta()



# COMMAND ----------

# DBTITLE 1,testing of salary of each department in descending order
def test_find_sal_dep():
    expected_result= ['employee_id', 'employee_name', 'department', 'country', 'salary', 'age', 'rank']
    db_name="employee_info"
    table_name="dim_employee"
    path=f"dbfs:/FileStore/assignments/assign_1/silver/{db_name}/{table_name}"
    delta_df = read_delta(path)

    desc_sal=find_sal_dep(delta_df)
    actual_result=desc_sal.columns
    logging.info(actual_result==expected_result)
test_find_sal_dep()

# COMMAND ----------

# DBTITLE 1,testing of function department names along with their corresponding country names.
def test_df_joins():
    expected_result= ['departmentName', 'countryName']

    country_path_read='dbfs:/FileStore/assignments/assign_1/source_to_bronze/country_df.csv/part-00000-tid-2429375358653570560-a09a881c-cda1-4428-be38-29a720a67fe8-317-1-c000.csv'
    department_path_read='dbfs:/FileStore/assignments/assign_1/source_to_bronze/department_df.csv/part-00000-tid-826393801858667356-6bf8e7e8-db95-4ea3-978c-de0c431bcb79-318-1-c000.csv'
    employee_path='dbfs:/FileStore/assignments/assign_1/source_to_bronze/employee_df.csv/part-00000-tid-8958534801557983101-843e525b-d29c-4eeb-8f0d-87dcd37dad59-319-1-c000.csv'

    country_df=read_schema(country_path_read,country_schema)
    department_df=read_schema(department_path_read,department_schema)
    employee_df=read_schema(employee_path,employee_schema)
    df1=df_joins(employee_df,country_df,department_df)
    dep_contry_df=df1.orderBy('departmentName')

    actual_result=dep_contry_df.columns
    logging.info(actual_result==expected_result)
test_df_joins()

# COMMAND ----------

# DBTITLE 1,testing of function for finding avg
def test_find_avg():
    expected_result=['department', 'avg_age', 'age']
    db_name="employee_info"
    table_name="dim_employee"
    path=f"dbfs:/FileStore/assignments/assign_1/silver/{db_name}/{table_name}"
    delta_df = read_delta(path) 

    avg_age_df=find_avg(delta_df,Window)
    actual_result=avg_age_df.columns
    logging.info(actual_result==expected_result)
test_find_avg()

# COMMAND ----------

# DBTITLE 1,testing for add column with current date
def test_add_load_data():
    expected_result= ['countryCode', 'countryName', 'at_load_data']
    country_path_read='dbfs:/FileStore/assignments/assign_1/source_to_bronze/country_df.csv/part-00000-tid-2429375358653570560-a09a881c-cda1-4428-be38-29a720a67fe8-317-1-c000.csv'
    country_df=read_schema(country_path_read,country_schema)
    col_name="at_load_data"
    country_df_date=add_load_data(country_df,col_name)

    actual_result=country_df_date.columns
    logging.info(actual_result==expected_result)
test_add_load_data()


# COMMAND ----------

# DBTITLE 1,testing of function write deltatable
def test_write_df_gold():
    expected_result= 'File found'
    employee_path='dbfs:/FileStore/assignments/assign_1/source_to_bronze/employee_df.csv/part-00000-tid-8958534801557983101-843e525b-d29c-4eeb-8f0d-87dcd37dad59-319-1-c000.csv'
    employee_df=read_schema(employee_path,employee_schema)
    col_name="at_load_data"
    employee_df_date=add_load_data(employee_df,col_name)

    folder='employee'
    table_name='fact_employee'
    df_name=employee_df_date
    path=f"dbfs:/FileStore/assignments/assign_1/gold/{folder}/{table_name}"

    write_df_gold(df_name,path)
    try:
        dirs = dbutils.fs.ls (path)
        actual_result='File found'
    except :
        actual_result="The File does not exist"
    logging.info(actual_result==expected_result)
test_write_df_gold()


