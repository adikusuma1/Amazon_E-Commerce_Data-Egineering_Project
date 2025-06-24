from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import col, trim, to_date, when, lit, count, upper

df = spark.read.format("delta")\
    .option("header", True)\
    .option("inferSchema", True)\
    .load("abfss://bronze@amazonprojdatalake.dfs.core.windows.net/amazon_sale_report")

df.display()

df_transformed = df.withColumn("index", col("index").cast("integer"))

df_transformed = df_transformed.withColumn("Order_ID", trim(col("Order_ID")))
df_transformed = df_transformed.withColumn("Date", to_date(col("Date"), "MM-dd-yy"))
df_transformed = df_transformed.withColumn(
    "Status",
    upper(trim(col("Status"))) 
)
df_transformed = df_transformed.withColumn(
    "Status",
    when(col("Status").like("SHIPPED - DELIVERED TO BUY%"), lit("DELIVERED")) 
    .when(col("Status").like("SHIPPED - RETURNED TO SELLER%"), lit("RETURNED"))
    .otherwise(col("Status"))
)
df_transformed = df_transformed.withColumn(
    "Courier_Status",
    when(col("Courier_Status").isNull(), lit("Unknown")) 
    .otherwise(col("Courier_Status")) 
)
df_transformed = df_transformed.withColumn("Qty", col("Qty").cast("integer"))
df_transformed = df_transformed.withColumn(
    "currency",
    when(col("currency").isNull(), lit("INR")).otherwise(col("currency"))
)
df_transformed = df_transformed.withColumn("Amount", col("Amount").cast("double"))
df_transformed = df_transformed.withColumn("Amount",
                              when(col("Amount").isNull(), lit(0.0))
                              .otherwise(col("Amount")))
exchange_rate_inr_to_usd = 0.012
df_transformed = df_transformed.withColumn("Amount_USD", col("Amount") * lit(exchange_rate_inr_to_usd))
df_transformed = df_transformed.withColumn(
    "ship_postal_code",
    col("ship_postal_code").cast("integer").cast("string")
)
df_transformed = df_transformed.withColumn(
    "B2B",
    when(col("B2B") == "False", False) 
    .otherwise(col("B2B").cast("boolean"))
)
df_transformed = df_transformed.withColumn(
    "promotion_ids",
    when(col("promotion_ids").isNull(), lit("No Promotion")) 
    .otherwise(col("promotion_ids")) 
)

from pyspark.sql.functions import split
df_transformed = df_transformed.withColumn("promotion_list", split(col("promotion_ids"), ","))
df_transformed = df_transformed.withColumn(
    "fulfilled_by",
    when(col("fulfilled_by").isNull(), lit("Unknown"))
    .otherwise(col("fulfilled_by"))
)
df_transformed = df_transformed.drop("Unnamed__22")
columns_to_fill_null_with_unknown = ["ship_city", "ship_state", "ship_postal_code", "ship_country"]

for col_name in columns_to_fill_null_with_unknown:
    df_transformed = df_transformed.withColumn(
        col_name,
        when(col(col_name).isNull(), lit("Unknown")).otherwise(col(col_name))
    )


df_transformed = df_transformed.withColumn(
    "Status",
    upper(trim(col("Status"))) 
)
df_transformed = df_transformed.withColumn(
    "Status", 
    when(col("Status") == "RETURNED", lit("RETURNED"))
    .when(col("Status") == "DELIVERED", lit("DELIVERED"))
    .when(col("Status") == "CANCELLED", lit("CANCELLED"))
    .when(col("Status") == "SHIPPED - REJECTED BY BUYER", lit("REJECTED"))
    .when(col("Status") == "SHIPPED - PICKED UP", lit("PICKED UP"))
    .when(col("Status") == "SHIPPED - OUT FOR DELIVERY", lit("OUT FOR DELIVERY"))
    .when(col("Status").like("SHIPPED - RETURNING TO SELLER%"), lit("RETURNED")) 
    .when(col("Status") == "SHIPPED - LOST IN TRANSIT", lit("LOST IN TRANSIT"))
    .when(col("Status") == "SHIPPED", lit("SHIPPED")) 
    .otherwise(col("Status")) 
)

df_transformed = df_transformed.drop("Status_Cleaned")

columns_to_exclude = ["index", "Order_ID", "Date", "Style", "SKU", "ASIN","Amount","ship_city","ship_state", "ship_postal_code", "promotion_ids", "_rescued_data", "Amount_USD", "promotion_list"]
for column_name in df_transformed.columns:
    if column_name not in columns_to_exclude:
        print(f"--- Nilai Unik untuk Kolom: {column_name} ---")
        df_transformed.select(col(column_name)).distinct().show(truncate=False)
        print("\n")

df_transformed = df_transformed.withColumn("Fulfilment_Flag", when(col('Fulfilment')=='Merchant',2)\
    .when(col('Fulfilment')=='Amazon',1)\
    .otherwise(0))

from pyspark.sql.window import Window

df_transformed = df_transformed.withColumn("sales_ranking",dense_rank().over(Window.orderBy(col('Amount_USD').desc())))
df_transformed.display()

df_transformed.createOrReplaceTempView("temp_view")
df_transformed.createOrReplaceGlobalTempView("global_view")
df_transformed = spark.sql("""
                           select * from global_temp.global_view
                           """)

df_agg = df_transformed.groupBy("Category").agg(
    sum("Amount_USD").alias("Total_Sales_Amount_USD"),
    sum("Qty").alias("Total_Item_Sold")
)

df_agg.display()

df_transformed.write.format("delta")\
    .mode("overwrite")\
        .option("path","abfss://silver@amazonprojdatalake.dfs.core.windows.net/amazon_sale_report")\
        .save()