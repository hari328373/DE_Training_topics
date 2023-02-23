# Databricks notebook source
# MAGIC %run ./Data_profiling_sample

# COMMAND ----------

df_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("education", StringType(), True),
    StructField("occupation", StringType(), True),
    StructField("income", IntegerType(), True),
    StructField("street_address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
])

# COMMAND ----------

path = 'dbfs:/FileStore/tables/data_profilng/sample_data.csv'
null_cols = ['id','first_name','last_name','gender','age','education','occupation','income','street_address','city','state']
numeric_cols = ['age','income']
aggregate_cols=['occupation','state']
result_limit = 100
data_quality_cols_regex = {'age': '^[0-99]{1,2}$', 'first_name': '^[a-zA-Z]*$', 'gender': '^M(ale)?$|^F(emale)?$'}



# COMMAND ----------

print("sample dataframe")
create_df(df_schema,path)
#display(df)

# COMMAND ----------

# 1. NULL Checks
print("NULL/Empty Percentage for Columns")
null_perc_df = get_null_perc(df,null_cols)
#display(null_perc_df)

# COMMAND ----------

#2. Summary, Average, Standard Deviation, Percentiles for Numeric Columns
print("Summary for Numeric Columns")
sum_numeric_df = get_summary_numeric(df,numeric_cols)
#display(sum_numeric_df)

# COMMAND ----------

#3. Distinct Count
print("Distinct Counts for Aggregate Columns")
dis_count_df = get_distinct_counts(df,aggregate_cols)
#display(dis_count_df)

# COMMAND ----------

#4. Distribution Count
print("Distribution Count for Aggregate Columns")
distribute_count = get_distribution_counts(df, aggregate_cols)
for i in distribute_count:
    print("Distribution for - " + i.columns[0])
    display(i)

# COMMAND ----------

#5. Data Quality
print("Data Quality Issue Percentage for Columns")
mismatch_perc_df = get_mismatch_perc(spark, df, data_quality_cols_regex)
#display(mismatch_perc_df)

# COMMAND ----------

