
# ==========================================================
# Azure Retail Data Engineering Pipeline
# Silver to Gold Transformation
# ==========================================================
#
# Reads curated Silver layer Delta tables, creates a
# fact table and analytical aggregates, and writes
# them to the Gold layer.
# ==========================================================

# ==========================================================
# Read Silver Layer
# ==========================================================

silver_path = "abfss://<container>@<storage-account>.dfs.core.windows.net/silver"

df_customers = spark.read.format("delta").load(f"{silver_path}/customers")
df_transactions = spark.read.format("delta").load(f"{silver_path}/transactions")
df_stores = spark.read.format("delta").load(f"{silver_path}/stores")
df_products = spark.read.format("delta").load(f"{silver_path}/products")

from pyspark.sql.functions import *

# ==========================================================
# Create Fact Sales Table
# ==========================================================

df_gold = (
    df_transactions
    .join(df_customers, "customer_id", "left")
    .join(df_products, "product_id", "left")
    .join(df_stores, "store_id", "left")
)

display(df_gold)

df_gold = df_gold.withColumn(
    "customer_name",
    concat_ws(" ", col("first_name"), col("last_name"))
)

df_gold = df_gold.withColumn(
    "total_sales",
    round(col("quantity") * col("price"), 2)
)

df_gold = df_gold.withColumn(
    "sales_year",
    year(col("transaction_date"))
)

df_gold = df_gold.withColumn(
    "sales_month",
    month(col("transaction_date"))
)

df_gold = df_gold.drop("first_name", "last_name")

display(df_gold)

# ==========================================================
# Write Gold Layer
# ==========================================================

gold_path = "abfss://<container>@<storage-account>.dfs.core.windows.net/gold"

df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_path}/factsales")

# ==========================================================
# Create Aggregate Tables
# ==========================================================

sales_by_store = df_gold.groupBy(
    "store_name",
    "location"
).agg(
    sum("total_sales").alias("total_revenue"),
    sum("quantity").alias("total_quantity"),
    count("transaction_id").alias("total_transactions")
)

# ==========================================================
# Write Gold Layer
# ==========================================================

sales_by_store.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_path}/sales_by_store")

#bycategory
sales_by_category = df_gold.groupBy(
    "category"
).agg(
    sum("total_sales").alias("total_revenue"),
    sum("quantity").alias("total_quantity")
)

# ==========================================================
# Write Gold Layer
# ==========================================================

sales_by_category.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_path}/sales_by_category")

#custsales
customer_sales = df_gold.groupBy(
    "customer_id",
    "customer_name"
).agg(
    sum("total_sales").alias("total_spent"),
    count("transaction_id").alias("total_purchases")
)

# ==========================================================
# Write Gold Layer
# ==========================================================

customer_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_path}/customer_sales")

#monthlysales
monthly_sales = df_gold.groupBy(
    "sales_year",
    "sales_month",
).agg(
    sum("total_sales").alias("total_revenue"),
    sum("quantity").alias("total_quantity")
).orderBy(
    "sales_year",
    "sales_month"
)

# ==========================================================
# Write Gold Layer
# ==========================================================

monthly_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_path}/monthly_sales")


df_factsales = spark.read.format("delta").load(
    f"{gold_path}/factsales"
)

display(df_factsales)
