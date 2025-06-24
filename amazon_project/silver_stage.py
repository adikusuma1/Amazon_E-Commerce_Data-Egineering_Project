from pyspark.sql.functions import lit, regexp_replace, col
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql import functions as F
from pyspark.sql.functions import regexp_extract

dbutils.widgets.text("source_folder", "Cloud Warehouse Compersion Chart")
dbutils.widgets.text("target_folder", "Cloud Warehouse Compersion Chart")

src_folder = dbutils.widgets.get("source_folder")
tgt_folder = dbutils.widgets.get("source_folder")

df = spark.read.format("csv")\
        .option("header", True)\
        .option("inferSchema", True)\
        .load(f"abfss://bronze@amazonprojdatalake.dfs.core.windows.net/{src_folder}")

for column in df.columns:
    new_name = column.replace(" ", "_")\
                     .replace("-", "_")\
                     .replace(":", "_")\
                     .replace(".", "")\
                     .strip()
    df = df.withColumnRenamed(column, new_name)

folder_key = src_folder.lower().strip().replace(" ", "_")

from pyspark.sql.functions import col, to_date, regexp_replace

# TRANSFORMASI SPESIFIK PER FOLDER
if folder_key == "international_sale_report":
    df = df.withColumn("DATE", to_date(col("DATE"), "dd-MM-yy"))
    df = df.withColumn("PCS", col("PCS").cast("int"))
    df = df.withColumn("RATE", col("RATE").cast("double"))
    df = df.withColumn("GROSS_AMT", col("GROSS_AMT").cast("double"))

elif folder_key == "sale_report":
    df = df.withColumnRenamed("SKU_Code", "SKU")\
           .withColumn("Stock", col("Stock").cast("double"))

elif folder_key == "may_2022":
    df = df.withColumnRenamed("Sku", "SKU")\
           .withColumnRenamed("Style Id", "Style")\
           .withColumnRenamed("MRP Old", "MRP_Old")\
           .withColumnRenamed("Final MRP Old", "Final_MRP_Old")

    df = df.withColumn("Size", regexp_extract("SKU", r"_([A-Z0-9]+)$", 1))

    harga_cols = ["TP", "MRP_Old", "Final_MRP_Old", "Ajio_MRP", "Amazon_MRP", "Amazon_FBA_MRP",
                  "Flipkart_MRP", "Limeroad_MRP", "Myntra_MRP", "Paytm_MRP", "Snapdeal_MRP"]
    for colname in harga_cols:
        df = df.withColumn(colname, col(colname).cast("double"))

elif folder_key == "p_l_march_2021":
    df = df.withColumnRenamed("Sku", "SKU")\
           .withColumnRenamed("Style Id", "Style")\
           .withColumnRenamed("MRP Old", "MRP_Old")\
           .withColumnRenamed("Final MRP Old", "Final_MRP_Old")
    harga_cols = ["TP_1", "TP_2", "MRP_Old", "Final_MRP_Old", "Ajio_MRP", "Amazon_MRP", "Amazon_FBA_MRP",
                  "Flipkart_MRP", "Limeroad_MRP", "Myntra_MRP", "Paytm_MRP", "Snapdeal_MRP"]
    for colname in harga_cols:
        df = df.withColumn(colname, col(colname).cast("double"))

elif folder_key == "expense_iigf":
    df = df.filter(~col("Recived_Amount").rlike("(?i)Particular|Total|Pending"))\
           .withColumnRenamed("Recived_Amount", "Received_Date")\
           .withColumnRenamed("Unnamed__1", "Received_Amount")\
           .withColumnRenamed("Expance", "Expense_Name")\
           .withColumnRenamed("Unnamed__3", "Expense_Amount")

    df = df.withColumn("Received_Amount", col("Received_Amount").cast("double"))\
           .withColumn("Expense_Amount", col("Expense_Amount").cast("double"))

elif folder_key == "cloud_warehouse_compersion_chart":
    df = df.filter(~col("Shiprocket").contains("Heads"))  
    df = df.withColumnRenamed("Unnamed__1", "Shiprocket_Price")\
           .withColumnRenamed("INCREFF", "Increff_Price")\
           .withColumn("Shiprocket_Price", regexp_replace("Shiprocket_Price", "₹", "").cast("string"))

# Drop baris kosong penuh
df = df.dropna(how="all")

df.write.format("delta")\
    .mode("append")\
    .option("mergeSchema", "true")\
    .option("path",f"abfss://silver@amazonprojdatalake.dfs.core.windows.net/{tgt_folder}")\
    .save()