# Databricks notebook source
# DBTITLE 1,all imports
from pyspark.sql.types import StructType,StringType,StructField,IntegerType
from pyspark.sql.functions import current_date
from pyspark.sql.window import Window
from pyspark.sql.functions import rank,avg
import logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

# COMMAND ----------

# DBTITLE 1,read csv function
def read_csv_data(path):
    df=spark.read.csv(path,header=True)
    return df

# COMMAND ----------

# DBTITLE 1,write csv function
def write_csv_file(df,path):
    df.write.format("csv").save(path)

# COMMAND ----------

# DBTITLE 1,read csv by custom schema
def read_schema(path,schema):
    df=spark.read.csv(path=path,header="false",schema=schema)
    return df

# COMMAND ----------

# DBTITLE 1,read csv by option methord
def read_schema_options(path,schema):
    df=spark.read.format("csv").options(header=False).schema(schema).load(path)
    return df

# COMMAND ----------

# DBTITLE 1,convert column name to snake case from Camel case
# 5. convert the Camel case of the columns to the snake case .

def camel_to_snake(df):
    for column in df.columns:
        new=""
        for i in column:
            if i.islower():
                new=new+"".join(i)
            else:
                temp=f"_{i}"
                new=new+"".join(temp.lower())
        df = df.withColumnRenamed(column,new)
    return df

# COMMAND ----------

# DBTITLE 1,add column with current date
def add_load_data(df,col):
    df=df.withColumn(col,current_date())
    return df

# COMMAND ----------

# DBTITLE 1,write df as delta table
def write_df(table_name,df,path):
    df.write.option('path',path).saveAsTable(table_name)

# COMMAND ----------

# DBTITLE 1,read delta table
def read_delta(path):
    df = spark.read.format("delta").load(path)
    return df

# COMMAND ----------

# DBTITLE 1,salary of each department in descending order.
def find_sal_dep(delta_df):
    windowPartition = Window.partitionBy("department").orderBy("salary")
    df=delta_df.withColumn("rank",rank().over(windowPartition))
    return df

# COMMAND ----------

# DBTITLE 1,department names along with their corresponding country names.
def df_joins(employee_df,country_df,department_df):
    df=employee_df.join(country_df,employee_df['country']==country_df['countryCode'],'inner')
    df=df.join(department_df,df['department']==department_df['departmentId'],"inner")
    return df.select(df.departmentName,df.countryName)


# COMMAND ----------

# DBTITLE 1,average age of employees in each department
def find_avg(delta_df,Window):
    windowParti=Window.partitionBy('department')
    df1=delta_df.withColumn("avg_age",avg(delta_df['age']).over(windowParti))
    return (df1.select(df1.department,df1.avg_age,df1.age)) 

# COMMAND ----------

# DBTITLE 1,write deltatable with replacewhere function
def write_df_gold(df_date,path):
    df_date.write.mode("overwrite").option("replaceWhere", "at_load_data >= '2024-01-01' ").save(path)
