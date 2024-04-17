# Databricks Assignment

## Question 1: Processing CSV Files


1. **Folder Creation:**
   - Created 3 folders: source_to_bronze, bronze_to_silver, silver_to_gold.

2. **Notebook Creation:**
   - Developed 4 notebooks in sequence:
     - 2 Notebooks in source_to_bronze: utils (for common functions) and employee_source_to_bronze (driver notebook).
     - 1 Notebook in bronze_to_silver: employee_bronze_to_silver.
     - 1 Notebook in silver_to_gold: employee_silver_to_gold.

3. **Data Ingestion and Transformation (employee_source_to_bronze):**
   - Read the 3 datasets as DataFrames.
   - Utilized functions from the utils notebook.
   - Saved the transformed DataFrames to DBFS in CSV format under /source_to_bronze/.

4. **Bronze Layer Transformation (employee_bronze_to_silver):**
   - Read the CSV file from DBFS location source_to_bronze with custom schema.
   - Converted column names from Camel case to snake case using UDF.
   - Added a load_date column with the current date.
   - Write the DF as a delta table to the location /silver/db_name/table_name.

5. **Gold Layer Transformation (employee_silver_to_gold):**
   - Read the Delta table from the silver layer.
   - Performed various analyses:
     - Salary of each department in descending order.
     - Number of employees in each department located in each country.
     - List of department names with corresponding country names.
     - Average age of employees in each department.
   - Added an at_load_date column to DataFrames.
   - Overwrote and replaced the data in /gold/employee/table_name(fact_employee) based on at_load_date.

## Question 2: API Data Processing


1. **API Data Retrieval:**
   - Fetched data from the provided API by passing parameters as page until the data is empty.
   - Api: https://reqres.in/api/users?page=2

2. **Data Processing:**
   - Read the data frame with a custom schema.
   - Flattened the DataFrame.
   - Derived a new column named site_address from the email field.
   - Added a load_date column with the current date.

3. **Data Storage:**
   - Saved the DataFrame in DBFS as /db_name /table_name in Delta format with overwrite mode.