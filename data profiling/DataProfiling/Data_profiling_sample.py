# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

def create_df(schema,path):
    return spark.read.format('csv').option('header',True).schema(schema).load(path)

# COMMAND ----------


def get_null_perc(df, null_cols):
    schema = StructType([ \
        StructField("Column",StringType(),True), \
        StructField("NullPercentage",StringType(),True)
    ])
    emptyRDD = spark.sparkContext.emptyRDD()
    resultdf = spark.createDataFrame(emptyRDD, schema=schema)
    for x in null_cols:
        if x.upper() in (name.upper() for name in df.columns):
            df_null_count = df.select(F.col(x)).filter(F.col(x).isNull() | (F.col(x) == '')).count()
            df_null = spark.createDataFrame([[x, str(df_null_count*100.0/df.count()) + '%' ]],schema=schema)
            resultdf = resultdf.union(df_null)
    return resultdf

# COMMAND ----------

def get_summary_numeric(df, numeric_cols):
    return df.select(numeric_cols).summary()

# COMMAND ----------

def get_distinct_counts(df, aggregate_cols):
    schema = StructType([ \
        StructField("Column",StringType(),True), \
        StructField("DistinctCount",StringType(),True)
    ])

    emptyRDD = spark.sparkContext.emptyRDD()
    resultdf = spark.createDataFrame(emptyRDD, schema=schema)

    for x in aggregate_cols:
        if x.upper() in (name.upper() for name in df.columns):
            df_distinct_count = df.select(F.col(x)).distinct().count()
            df_distinct = spark.createDataFrame([[x, str(df_distinct_count)]],schema=schema)
            resultdf = resultdf.union(df_distinct)

    return resultdf

# COMMAND ----------

def get_distribution_counts(df, aggregate_cols):
    result = []
    for x in aggregate_cols:
        if x.upper() in (name.upper() for name in df.columns):
            result.append(df.groupby(F.col(x)).count().sort(F.col("count").desc()))
    return result


# COMMAND ----------

def get_mismatch_perc(spark, df, data_quality_cols_regex):
    schema = StructType([ \
        StructField("Column",StringType(),True), \
        StructField("MismatchPercentage",StringType(),True)
    ])

    emptyRDD = spark.sparkContext.emptyRDD()
    resultdf = spark.createDataFrame(emptyRDD, schema=schema)


    for key, value in data_quality_cols_regex.items():
        if key.upper() in (name.upper() for name in df.columns):
            df_regex_not_like_count = df.select(F.col(key)).filter(~F.col(key).rlike(value)).count()
            df_regex_not_like = spark.createDataFrame([[key, str(df_regex_not_like_count*100.0/df.count()) + '%']],schema=schema)
            resultdf = resultdf.union(df_regex_not_like)

    return resultdf

# COMMAND ----------

