%sql
CREATE SCHEMA IF NOT EXISTS amazon_catalog.net_schema;

checkpoint_location = "abfss://silver@amazonprojdatalake.dfs.core.windows.net/checkpoint"

df = spark.readStream\
    .format("Cloudfiles")\
    .option("cloudFiles.format", "csv")\
    .option("cloudFiles.schemaLocation",checkpoint_location)\
    .load("abfss://raw@amazonprojdatalake.dfs.core.windows.net")
    

from pyspark.sql.functions import col
for column in df.columns:
    new_name = column.replace(" ", "_").replace("-", "_").replace(":", "_").strip()
    df = df.withColumnRenamed(column, new_name)

df = df.withColumnRenamed("Sales_Channel_", "Sales_Channel") \
       .withColumnRenamed("promotion_ids", "promotion_ids") 

df.writeStream\
    .option("checkpointLocation", checkpoint_location)\
    .trigger(processingTime='10 seconds')\
    .start("abfss://bronze@amazonprojdatalake.dfs.core.windows.net/amazon_sale_report")

