from pyspark.sql.functions import *

@dlt.table(
    name = "gold_amazon_warehouse_costs"
)

def gold_amazon_warehouse_costs():
    df = spark.readStream.format("delta").load("abfss://silver@amazonprojdatalake.dfs.core.windows.net/Cloud Warehouse Compersion Chart")
    return df

@dlt.table(
    name = "gold_amazon_expense_iigf"
)

def gold_amazon_expense_iigf():
    df = spark.readStream.format("delta").load("abfss://silver@amazonprojdatalake.dfs.core.windows.net/Expense IIGF")
    return df

@dlt.table(
    name = "gold_amazon_international_sales"
)

def gold_amazon_international_sales():
    df = spark.readStream.format("delta").load("abfss://silver@amazonprojdatalake.dfs.core.windows.net/International sale Report")
    return df

@dlt.table(
    name = "gold_amazon_pricing_may2022"
)

def gold_amazon_pricing_may2022():
    df = spark.readStream.format("delta").load("abfss://silver@amazonprojdatalake.dfs.core.windows.net/May-2022")
    return df

@dlt.table(
    name = "gold_amazon_pricing_mar2021"
)

def gold_amazon_pricing_mar2021():
    df = spark.readStream.format("delta").load("abfss://silver@amazonprojdatalake.dfs.core.windows.net/P  L March 2021")
    return df

@dlt.table(
    name = "gold_amazon_stock_report"
)

def gold_amazon_stock_report():
    df = spark.readStream.format("delta").load("abfss://silver@amazonprojdatalake.dfs.core.windows.net/Sale Report")
    return df

@dlt.table

def gold_stg_amazon_sale_report():
    df = spark.readStream.format("delta").load("abfss://silver@amazonprojdatalake.dfs.core.windows.net/amazon_sale_report")
    return df

@dlt.view

def gold_transform_amazon_sale_report():
    df = spark.readStream.table("LIVE.gold_stg_amazon_sale_report")
    df = df.withColumn("newflag",lit(1))
    return df

master_data_rules = {
    "rule1":"newflag IS NOT NULL"
}

@dlt.table

@dlt.expect_all_or_drop(master_data_rules)
def gold_amazon_sale_report():
    df = spark.readStream.table("LIVE.gold_transform_amazon_sale_report")
    return df