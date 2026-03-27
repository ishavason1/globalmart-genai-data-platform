# Databricks notebook source
# MAGIC %md
# MAGIC Bronze Layer: Raw ingestion using Auto Loader
# MAGIC Handles schema drift, ensures incremental processing, and maintains full traceability

# COMMAND ----------

# Customers Ingestion (Dynamic for ALL Regions)
from pyspark.sql.functions import current_timestamp, col

customers_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", "/Volumes/globalmart/bronze/raw_data/schema/customers")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.inferColumnTypes", "true")
    .load("/Volumes/globalmart/bronze/raw_data/Region */customers_*.csv")
    
    # Add metadata columns
    .withColumn("source_file", col("_metadata.file_path"))
    .withColumn("ingestion_timestamp", current_timestamp())
)

customers_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/Volumes/globalmart/bronze/raw_data/checkpoints/customers") \
    .trigger(availableNow=True) \
    .toTable("globalmart.bronze.customers")

# COMMAND ----------

# Orders
orders_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", "/Volumes/globalmart/bronze/raw_data/schema/orders")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.inferColumnTypes", "true")
    .load("/Volumes/globalmart/bronze/raw_data/Region */orders_*.csv")
    
    .withColumn("source_file", col("_metadata.file_path"))
    .withColumn("ingestion_timestamp", current_timestamp())
)

orders_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/Volumes/globalmart/bronze/raw_data/checkpoints/orders") \
    .trigger(availableNow=True) \
    .toTable("globalmart.bronze.orders")

# COMMAND ----------

# Transactions
transactions_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", "/Volumes/globalmart/bronze/raw_data/schema/transactions")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.inferColumnTypes", "true")
    .load("/Volumes/globalmart/bronze/raw_data/transactions_*.csv")
    
    .withColumn("source_file", col("_metadata.file_path"))
    .withColumn("ingestion_timestamp", current_timestamp())
)

transactions_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/Volumes/globalmart/bronze/raw_data/checkpoints/transactions") \
    .trigger(availableNow=True) \
    .toTable("globalmart.bronze.transactions")

# COMMAND ----------

# products

from pyspark.sql.functions import current_timestamp, col

products_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/Volumes/globalmart/bronze/raw_data/schema/products")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.inferColumnTypes", "true")
    
    .option("pathGlobFilter", "products.json")
    
    .option("multiLine", "true")
    
    .load("/Volumes/globalmart/bronze/raw_data/")
    
    .withColumn("source_file", col("_metadata.file_path"))
    .withColumn("ingestion_timestamp", current_timestamp())
)

products_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/Volumes/globalmart/bronze/raw_data/checkpoints/products") \
    .trigger(availableNow=True) \
    .toTable("globalmart.bronze.products")


# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

returns_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/Volumes/globalmart/bronze/raw_data/schema/returns")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")

    
    .option("multiLine", "true")
    
    
    .option("cloudFiles.inferColumnTypes", "true")
    
    .load("/Volumes/globalmart/bronze/raw_data/returns_*.json")
    
    .withColumn("source_file", col("_metadata.file_path"))
    .withColumn("ingestion_timestamp", current_timestamp())
)

returns_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/Volumes/globalmart/bronze/raw_data/checkpoints/returns") \
    .trigger(availableNow=True) \
    .toTable("globalmart.bronze.returns")

# COMMAND ----------

# Vendors

from pyspark.sql.functions import current_timestamp, col

vendors_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", "/Volumes/globalmart/bronze/raw_data/schema/vendors")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("pathGlobFilter", "vendors.csv")
    
    .load("/Volumes/globalmart/bronze/raw_data/")
    
    .withColumn("source_file", col("_metadata.file_path"))
    .withColumn("ingestion_timestamp", current_timestamp())
)

vendors_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/Volumes/globalmart/bronze/raw_data/checkpoints/vendors") \
    .trigger(availableNow=True) \
    .toTable("globalmart.bronze.vendors")