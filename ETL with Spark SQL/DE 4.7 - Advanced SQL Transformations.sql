-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC # Advanced SQL Transformations
-- MAGIC 
-- MAGIC Querying tabular data stored in the data lakehouse with Spark SQL is easy, efficient, and fast.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-4.7

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Interacting with JSON Data
-- MAGIC 
-- MAGIC Here we are using the kafka payload as a sample data

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW events_strings AS
  SELECT string(key), string(value) 
  FROM events_raw;
  
SELECT * FROM events_strings

-- COMMAND ----------

describe events_strings

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC Spark SQL has built-in functionality to directly interact with JSON data stored as strings. We can use the **`:`** syntax to traverse nested data structures.

-- COMMAND ----------

SELECT value:* 
FROM events_strings

-- COMMAND ----------

SELECT value:device, value:geo:city 
FROM events_strings

-- COMMAND ----------

SELECT value 
FROM events_strings 
WHERE value:event_name = "finalize" 
ORDER BY key
LIMIT 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC Spark SQL also has the ability to parse JSON objects into struct types (a native Spark type with nested attributes).
-- MAGIC 
-- MAGIC However, the **`from_json`** function requires a schema. To derive the schema of our current data, we'll start by executing a query we know will return a JSON value with no null fields.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC Spark SQL also has a **`schema_of_json`** function to derive the JSON schema from an example. Here, we copy and paste an example JSON to the function and chain it into the **`from_json`** function to cast our **`value`** field to a struct type.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW parsed_events AS
  SELECT from_json(value, schema_of_json('{"device":"Linux","ecommerce":{"purchase_revenue_in_usd":1075.5,"total_item_quantity":1,"unique_items":1},"event_name":"finalize","event_previous_timestamp":1593879231210816,"event_timestamp":1593879335779563,"geo":{"city":"Houston","state":"TX"},"items":[{"coupon":"NEWBED10","item_id":"M_STAN_K","item_name":"Standard King Mattress","item_revenue_in_usd":1075.5,"price_in_usd":1195.0,"quantity":1}],"traffic_source":"email","user_first_touch_timestamp":1593454417513109,"user_id":"UA000000106116176"}')) AS json 
  FROM events_strings;
  
SELECT * FROM parsed_events

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC Once a JSON string is unpacked to a struct type, Spark supports **`*`** (star) unpacking to flatten fields into columns.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW new_events_final AS
  SELECT json.* 
  FROM parsed_events;
  
SELECT * FROM new_events_final

-- COMMAND ----------

DESCRIBE events

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW new_events_final1 AS
  SELECT ecommerce.*, geo.*
  FROM new_events_final;
  
SELECT * FROM new_events_final1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Explode Arrays
-- MAGIC The **`explode`** function lets us put each element in an array on its own row.

-- COMMAND ----------

select * from events;

-- COMMAND ----------

SELECT user_id, explode(items)
FROM events;

-- COMMAND ----------

SELECT user_id, explode_col 
FROM events
LATERAL VIEW explode(items) excol as explode_col;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW new_events_explode AS
SELECT user_id, event_timestamp, event_name, explode(items) AS item 
FROM events;

select * from new_events_explode;

-- COMMAND ----------

describe new_events_explode;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW new_events_item_flatten AS
SELECT item.*
FROM new_events_explode;

select * from new_events_item_flatten;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC ## Collect Arrays
-- MAGIC 
-- MAGIC The **`collect_set`** function can collect unique values for a field, including fields within arrays.
-- MAGIC 
-- MAGIC The **`flatten`** function allows multiple arrays to be combined into a single array.
-- MAGIC 
-- MAGIC The **`array_distinct`** function removes duplicate elements from an array.
-- MAGIC 
-- MAGIC Here, we combine these queries to create a simple table that shows the unique collection of actions and the items in a user's cart.

-- COMMAND ----------

select * from events;

-- COMMAND ----------

SELECT user_id,
  collect_set(event_name) AS event_history
FROM events
GROUP BY user_id

-- COMMAND ----------

SELECT user_id,
  collect_set(items.item_id) AS cart_history
FROM events
GROUP BY user_id;

-- COMMAND ----------

SELECT user_id,
flatten(collect_set(items.item_id)) AS cart_history
FROM events
GROUP BY user_id

-- COMMAND ----------

SELECT user_id,
array_distinct(flatten(collect_set(items.item_id))) AS cart_history
FROM events
GROUP BY user_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC ## Join Tables
-- MAGIC 
-- MAGIC Spark SQL supports standard join operations (inner, outer, left, right, anti, cross, semi).
-- MAGIC 
-- MAGIC Here we chain a join with a lookup table to an **`explode`** operation to grab the standard printed item name.

-- COMMAND ----------

CREATE OR REPLACE VIEW sales_enriched AS
SELECT *
FROM (
  SELECT *, explode(items) AS item 
  FROM sales) a
INNER JOIN item_lookup b
ON a.item.item_id = b.item_id;

SELECT * FROM sales_enriched

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Set Operators
-- MAGIC Spark SQL supports **`UNION`**, **`MINUS`**, and **`INTERSECT`** set operators.
-- MAGIC 
-- MAGIC **`UNION`** returns the collection of two queries. 
-- MAGIC 
-- MAGIC The query below returns the same results as if we inserted our **`new_events_final`** into the **`events`** table.

-- COMMAND ----------

SELECT * FROM events 
UNION 
SELECT * FROM new_events_final

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC **`INTERSECT`** returns all rows found in both relations.

-- COMMAND ----------

SELECT * FROM events 
INTERSECT 
SELECT * FROM new_events_final

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **`MINUS`** returns all the rows found in one dataset but not the other

-- COMMAND ----------

SELECT * FROM events 
MINUS 
SELECT * FROM new_events_final

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC 
-- MAGIC ## Pivot Tables
-- MAGIC The **`PIVOT`** clause is used for data perspective. We can get the aggregated values based on specific column values, which will be turned to multiple columns used in **`SELECT`** clause. The **`PIVOT`** clause can be specified after the table name or subquery.
-- MAGIC 
-- MAGIC **`SELECT * FROM ()`**: The **`SELECT`** statement inside the parentheses is the input for this table.
-- MAGIC 
-- MAGIC **`PIVOT`**: The first argument in the clause is an aggregate function and the column to be aggregated. Then, we specify the pivot column in the **`FOR`** subclause. The **`IN`** operator contains the pivot column values. 
-- MAGIC 
-- MAGIC Here we use **`PIVOT`** to create a new **`transactions`** table that flattens out the information contained in the **`sales`** table.
-- MAGIC 
-- MAGIC This flattened data format can be useful for dashboarding, but also useful for applying machine learning algorithms for inference or prediction.

-- COMMAND ----------

select * from sales_enriched;

-- COMMAND ----------

CREATE OR REPLACE TABLE transactions AS

SELECT * FROM (
  SELECT
    email,
    order_id,
    transaction_timestamp,
    total_item_quantity,
    purchase_revenue_in_usd,
    unique_items,
    item.item_id AS item_id,
    item.quantity AS quantity
  FROM sales_enriched
) PIVOT (
  sum(quantity) FOR item_id in (
    'P_FOAM_K',
    'M_STAN_Q',
    'P_FOAM_S',
    'M_PREM_Q',
    'M_STAN_F',
    'M_STAN_T',
    'M_PREM_K',
    'M_PREM_F',
    'M_STAN_K',
    'M_PREM_T',
    'P_DOWN_S',
    'P_DOWN_K'
  )
);

SELECT * FROM transactions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Higher Order Functions
-- MAGIC 
-- MAGIC Higher order functions include:
-- MAGIC - **`FILTER`** filters an array using the given lambda function.
-- MAGIC - **`EXIST`** tests whether a statement is true for one or more elements in an array. 
-- MAGIC - **`TRANSFORM`** uses the given lambda function to transform all elements in an array.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Filter
-- MAGIC Remove items that are not king-sized from all records in our **`items`** column. We can use the **`FILTER`** function to create a new column that excludes that value from each array.
-- MAGIC 
-- MAGIC **`FILTER (items, i -> i.item_id LIKE "%K") AS king_items`**
-- MAGIC 
-- MAGIC In the statement above:
-- MAGIC - **`FILTER`** : the name of the higher-order function <br>
-- MAGIC - **`items`** : the name of our input array <br>
-- MAGIC - **`i`** : the name of the iterator variable. You choose this name and then use it in the lambda function. It iterates over the array, cycling each value into the function one at a time.<br>
-- MAGIC - **`->`** :  Indicates the start of a function <br>
-- MAGIC - **`i.item_id LIKE "%K"`** : This is the function. Each value is checked to see if it ends with the capital letter K. If it is, it gets filtered into the new column, **`king_items`**

-- COMMAND ----------

-- filter for sales of only king sized items
SELECT
  order_id,
  items,
  FILTER (items, i -> i.item_id LIKE "%K") AS king_items
FROM sales

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC You may write a filter that produces a lot of empty arrays in the created column. When that happens, it can be useful to use a **`WHERE`** clause to show only non-empty array values in the returned column. 
-- MAGIC 
-- MAGIC In this example, we accomplish that by using a subquery (a query within a query). They are useful for performing an operation in multiple steps. In this case, we're using it to create the named column that we will use with a **`WHERE`** clause.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW king_size_sales AS

SELECT order_id, king_items
FROM (
  SELECT
    order_id,
    FILTER (items, i -> i.item_id LIKE "%K") AS king_items
  FROM sales)
WHERE size(king_items) > 0;
  
SELECT * FROM king_size_sales

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Exists
-- MAGIC Tests whether a statement is true for one or more elements in an array.

-- COMMAND ----------

-- filter for sales of only king sized items
create or replace temp view v_exists as
SELECT
  order_id,
  items,
  EXISTS (items, i -> i.item_id LIKE "%K") AS king_items
FROM sales;
select * from v_exists

-- COMMAND ----------

select *
from v_exists
where king_items=="true"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Transform
-- MAGIC Built-in functions are designed to operate on a single, simple data type within a cell; they cannot process array values. **`TRANSFORM`** can be particularly useful when you want to apply an existing function to each element in an array. 
-- MAGIC 
-- MAGIC Compute the total revenue from king-sized items per order.
-- MAGIC 
-- MAGIC **`TRANSFORM(king_items, k -> CAST(k.item_revenue_in_usd * 100 AS INT)) AS item_revenues`**
-- MAGIC 
-- MAGIC In the statement above, for each value in the input array, we extract the item's revenue value, multiply it by 100, and cast the result to integer. Note that we're using the same kind as references as in the previous command, but we name the iterator with a new variable, **`k`**.

-- COMMAND ----------

-- get total revenue from king items per order
CREATE OR REPLACE TEMP VIEW king_item_revenues AS

SELECT
  order_id,
  king_items,
  TRANSFORM (
    king_items,
    k -> CAST(k.item_revenue_in_usd * 100 AS INT)
  ) AS item_revenues
FROM king_size_sales;

SELECT * FROM king_item_revenues


-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Summary
-- MAGIC Spark SQL offers a comprehensive set of native functionality for interacting with and manipulating highly nested data.
-- MAGIC 
-- MAGIC While some syntax for this functionality may be unfamiliar to SQL users, leveraging built-in functions like higher order functions can prevent SQL engineers from needing to rely on custom logic when dealing with highly complex data structures.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC Run the following cell to delete the tables and files associated with this lesson.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>

-- COMMAND ----------

select explode(array(1,2)), explode(array(3, 4))

-- COMMAND ----------

select
    a_exp.a,
    b_exp.b
from (select array(1, 2) as a, array(3, 4) as b) t
lateral view explode(t.a) a_exp as a
lateral view explode(t.b) b_exp as b

-- COMMAND ----------

