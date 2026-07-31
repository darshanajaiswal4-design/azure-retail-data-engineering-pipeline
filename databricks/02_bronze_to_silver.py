
# ==========================================================
# Azure Retail Data Engineering Pipeline
# Bronze to Silver Transformation
# ==========================================================
#
# Reads raw data from the Bronze layer, performs data
# cleansing and standardization, and writes Delta tables
# to the Silver layer.
# ==========================================================

# ==========================================================
# Read Bronze Layer
# ==========================================================

bronze_path = "abfss://<container>@<storage-account>.dfs.core.windows.net/bronze"

# Customer data (originally ingested from a JSON source)
df_customers = spark.read.parquet(
    f"{bronze_path}/Customers"
)

df_products = spark.read.parquet(
    f"{bronze_path}/Products"
)

df_stores = spark.read.parquet(
    f"{bronze_path}/Stores"
)

df_transactions = spark.read.parquet(
    f"{bronze_path}/Transactions"
)

# ==========================================================
# Data Cleansing & Standardization
# ==========================================================

from pyspark.sql.functions import col

# Convert types and clean data
df_transactions = df_transactions.select(
    col("transaction_id").cast("int"),
    col("customer_id").cast("int"),
    col("product_id").cast("int"),
    col("store_id").cast("int"),
    col("quantity").cast("int"),
    col("transaction_date").cast("date")
)

df_products = df_products.select(
    col("product_id").cast("int"),
    col("product_name"),
    col("category"),
    col("price").cast("double")
)

df_stores = df_stores.select(
    col("store_id").cast("int"),
    col("store_name"),
    col("location")
)

df_customers = df_customers.withColumn(
    "customer_id",
    col("customer_id").cast("int")
)

df_customers = df_customers.select(
    "customer_id", "first_name", "last_name", "email", "city", "registration_date"
).dropDuplicates(["customer_id"])


from pyspark.sql.functions import upper

df_customers = df_customers.withColumn(
    "city",
    upper(col("city"))
)

df_customers = df_customers.fillna({
    "city": "Unknown"
})

# ==========================================================
# Write Silver Layer
# ==========================================================

silver_path = "abfss://<container>@<storage-account>.dfs.core.windows.net/silver"

df_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{silver_path}/customers")

df_transactions.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{silver_path}/transactions")

df_stores.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{silver_path}/stores")

df_products.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{silver_path}/products")

display(df_customers)
display(df_transactions)
display(df_stores)
display(df_products)
