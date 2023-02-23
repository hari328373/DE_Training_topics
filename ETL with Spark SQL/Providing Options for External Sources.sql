-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC # Providing Options for External Sources
-- MAGIC While directly querying files works well for self-describing formats, many data sources require additional configurations or schema declaration to properly ingest records.
-- MAGIC 
-- MAGIC In this lesson, we will create tables using external data sources. While these tables will not yet be stored in the Delta Lake format (and therefore not be optimized for the Lakehouse), this technique helps to facilitate extracting data from diverse external systems.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Run Setup
-- MAGIC 
-- MAGIC The setup script will create the data and declare necessary values for the rest of this notebook to execute.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-4.2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## When Direct Queries Don't Work 
-- MAGIC 
-- MAGIC While views can be used to persist direct queries against files between sessions, this approach has limited utility.
-- MAGIC 
-- MAGIC CSV files are one of the most common file formats, but a direct query against these files rarely returns the desired results.

-- COMMAND ----------

SELECT * FROM csv.`${da.paths.working_dir}/sales-csv`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC We can see from the above that:
-- MAGIC 1. The header row is being extracted as a table row
-- MAGIC 1. All columns are being loaded as a single column
-- MAGIC 1. The file is pipe-delimited (**`|`**)
-- MAGIC 1. The final column appears to contain nested data that is being truncated

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Registering Tables on External Data with Read Options
-- MAGIC 
-- MAGIC While Spark will extract some self-describing data sources efficiently using default settings, many formats will require declaration of schema or other options.
-- MAGIC 
-- MAGIC While there are many <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-create-table-using.html" target="_blank">additional configurations</a> you can set while creating tables against external sources, the syntax below demonstrates the essentials required to extract data from most formats.
-- MAGIC 
-- MAGIC <strong><code>
-- MAGIC CREATE TABLE table_identifier (col_name1 col_type1, ...)<br/>
-- MAGIC USING data_source<br/>
-- MAGIC OPTIONS (key1 = val1, key2 = val2, ...)<br/>
-- MAGIC LOCATION = path<br/>
-- MAGIC </code></strong>
-- MAGIC 
-- MAGIC Note that options are passed with keys as unquoted text and values in quotes. Spark supports many <a href="https://docs.databricks.com/data/data-sources/index.html" target="_blank">data sources</a> with custom options, and additional systems may have unofficial support through external <a href="https://docs.databricks.com/libraries/index.html" target="_blank">libraries</a>. 
-- MAGIC 
-- MAGIC **NOTE**: Depending on your workspace settings, you may need administrator assistance to load libraries and configure the requisite security settings for some data sources.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC The cell below demonstrates using Spark SQL DDL to create a table against an external CSV source, specifying:
-- MAGIC 1. The column names and types
-- MAGIC 1. The file format
-- MAGIC 1. The delimiter used to separate fields
-- MAGIC 1. The presence of a header
-- MAGIC 1. The path to where this data is stored

-- COMMAND ----------

CREATE TABLE sales_csv
  (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
USING CSV
OPTIONS (
  header = "true",
  delimiter = "|"
)
LOCATION "${da.paths.working_dir}/sales-csv"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC Note that no data has moved during table declaration. Similar to when we directly queried our files and created a view, we are still just pointing to files stored in an external location.
-- MAGIC 
-- MAGIC Run the following cell to confirm that data is now being loaded correctly.

-- COMMAND ----------

SELECT * FROM sales_csv

-- COMMAND ----------

SELECT COUNT(*) FROM sales_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC All the metadata and options passed during table declaration will be persisted to the metastore, ensuring that data in the location will always be read with these options.
-- MAGIC 
-- MAGIC **NOTE**: When working with CSVs as a data source, it's important to ensure that column order does not change if additional data files will be added to the source directory. Because the data format does not have strong schema enforcement, Spark will load columns and apply column names and data types in the order specified during table declaration.
-- MAGIC 
-- MAGIC Running **`DESCRIBE EXTENDED`** on a table will show all of the metadata associated with the table definition.

-- COMMAND ----------

DESCRIBE EXTENDED sales_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Limits of Tables with External Data Sources
-- MAGIC 
-- MAGIC If you've taken other courses on Databricks or reviewed any of our company literature, you may have heard about Delta Lake and the Lakehouse. Note that whenever we're defining tables or queries against external data sources, we **cannot** expect the performance guarantees associated with Delta Lake and Lakehouse.
-- MAGIC 
-- MAGIC For example: while Delta Lake tables will guarantee that you always query the most recent version of your source data, tables registered against other data sources may represent older cached versions.
-- MAGIC 
-- MAGIC The cell below executes some logic that we can think of as just representing an external system directly updating the files underlying our table.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.table("sales_csv")
-- MAGIC       .write.mode("append")
-- MAGIC       .format("csv")
-- MAGIC       .save(f"{DA.paths.working_dir}/sales-csv"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC If we look at the current count of records in our table, the number we see will not reflect these newly inserted rows.

-- COMMAND ----------

SELECT COUNT(*) FROM sales_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC At the time we previously queried this data source, Spark automatically cached the underlying data in local storage. This ensures that on subsequent queries, Spark will provide the optimal performance by just querying this local cache.
-- MAGIC 
-- MAGIC Our external data source is not configured to tell Spark that it should refresh this data. 
-- MAGIC 
-- MAGIC We **can** manually refresh the cache of our data by running the **`REFRESH TABLE`** command.

-- COMMAND ----------

REFRESH TABLE sales_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC Note that refreshing our table will invalidate our cache, meaning that we'll need to rescan our original data source and pull all data back into memory. 
-- MAGIC 
-- MAGIC For very large datasets, this may take a significant amount of time.

-- COMMAND ----------

SELECT COUNT(*) FROM sales_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Extracting Data from SQL Databases
-- MAGIC SQL databases are an extremely common data source, and Databricks has a standard JDBC driver for connecting with many flavors of SQL.
-- MAGIC 
-- MAGIC The general syntax for creating these connections is:
-- MAGIC 
-- MAGIC <strong><code>
-- MAGIC CREATE TABLE <jdbcTable><br/>
-- MAGIC USING JDBC<br/>
-- MAGIC OPTIONS (<br/>
-- MAGIC &nbsp; &nbsp; url = "jdbc:{databaseServerType}://{jdbcHostname}:{jdbcPort}",<br/>
-- MAGIC &nbsp; &nbsp; dbtable = "{jdbcDatabase}.table",<br/>
-- MAGIC &nbsp; &nbsp; user = "{jdbcUsername}",<br/>
-- MAGIC &nbsp; &nbsp; password = "{jdbcPassword}"<br/>
-- MAGIC )
-- MAGIC </code></strong>
-- MAGIC 
-- MAGIC In the code sample below, we'll connect with <a href="https://www.sqlite.org/index.html" target="_blank">SQLite</a>.
-- MAGIC   
-- MAGIC **NOTE:** SQLite uses a local file to store a database, and doesn't require a port, username, or password.  
-- MAGIC   

-- COMMAND ----------

DROP TABLE IF EXISTS users_jdbc;

CREATE TABLE users_jdbc
USING JDBC
OPTIONS (
  url = "jdbc:sqlite:/${da.username}_ecommerce.db",
  dbtable = "users"
)

-- COMMAND ----------


SELECT * FROM users_jdbc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC Looking at the table metadata reveals that we have captured the schema information from the external system. Storage properties (which would include the username and password associated with the connection) are automatically redacted.

-- COMMAND ----------

DESCRIBE EXTENDED users_jdbc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC While the table is listed as **`MANAGED`**, listing the contents of the specified location confirms that no data is being persisted locally.