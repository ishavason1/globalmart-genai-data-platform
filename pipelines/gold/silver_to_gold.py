# ============================================================================
# GOLD LAYER – GLOBALMART LAKEHOUSE
# ============================================================================
# PURPOSE OF THIS NOTEBOOK
# ----------------------------------------------------------------------------
# The Gold layer represents the final, business-ready analytical model.
# Unlike Silver (which focuses on data quality and standardization),
# the Gold layer focuses on:
#
#   1. Clearly defined *business events* (fact tables)
#   2. Business context and descriptors (dimension tables)
#   3. Pre-aggregated metrics to support frequent business queries
#
# This layer directly addresses GlobalMart's three core business failures:
#   - Revenue audit inconsistencies
#   - Returns fraud detection
#   - Inventory blindspots across regions
#
# All tables in this notebook are implemented as STREAMING Delta Live Tables
# to ensure results are always current as new data arrives.
# ============================================================================

import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="dim_customer",
    comment="""
    Customer dimension table.
    
    Grain:
      - One row represents one unique customer.

    Business Purpose:
      - Enables analysis by customer segment (Consumer / Corporate / Home Office)
      - Supports fraud detection across regions
      - Provides customer context for revenue reporting

    Source:
      - Built from cleaned and deduplicated Silver-layer customer data.
    """
)
def dim_customer():

    # Read streaming Silver customer data.
    # Using dlt.read_stream ensures compatibility with upstream streaming tables
    customers = dlt.read_stream("globalmart.silver.customers_silver")

    # Select only descriptive attributes.
    # We intentionally do NOT include transactional fields here.
    return (
        customers
        .select(
            "customer_id",        # Primary business key
            "customer_name",
            "customer_email",
            "segment",            # Critical for profitability analysis
            "region"              # Region of customer's primary association
        )
        # Deduplicate to guarantee exactly one row per customer
        .dropDuplicates(["customer_id"])
    )


@dlt.table(
    name="dim_product",
    comment="""
    Product dimension table.

    Grain:
      - One row per product.

    Business Purpose:
      - Enables product-level revenue analysis
      - Supports inventory performance tracking
      - Allows grouping by category and brand

    Source:
      - Built from Silver-layer products after schema normalization.
    """
)
def dim_product():

    products = dlt.read_stream("globalmart.silver.products_silver")

    return (
        products
        .select(
            "product_id",          # Primary key
            "product_name",
            "categories",
            "brand",
            "upc"
        )
        .dropDuplicates(["product_id"])
    )

@dlt.table(
    name="dim_vendor",
    comment="""
    Vendor dimension table.

    Grain:
      - One row per vendor.

    Business Purpose:
      - Enables vendor performance analysis
      - Supports return-rate calculation by vendor
      - Helps identify problematic suppliers

    Source:
      - Built from Silver-layer vendors.
    """
)
def dim_vendor():

    vendors = dlt.read_stream("globalmart.silver.vendors_silver")

    return (
        vendors
        .select(
            "vendor_id",      # Vendor business key
            "vendor_name"
        )
        .dropDuplicates(["vendor_id"])
    )

@dlt.table(
    name="dim_date",
    comment="""
    Calendar date dimension.

    Grain:
      - One row per unique date.

    Business Purpose:
      - Enables month-over-month and quarter-over-quarter analysis
      - Ensures consistent time joins across facts
      - Avoids repeated date parsing logic in queries

    Source:
      - Derived from transactional order dates and return dates.
    """
)
def dim_date():

    # Extract dates from different business processes
    order_dates = dlt.read_stream("globalmart.silver.orders_silver") \
        .select(col("order_purchase_date").alias("date"))

    return_dates = dlt.read_stream("globalmart.silver.returns_silver") \
        .select(col("return_date").alias("date"))

    # Union ensures full temporal coverage
    all_dates = order_dates.unionByName(return_dates).dropDuplicates()

    return (
        all_dates
        # Surrogate-style date key for joins
        .withColumn("date_key", date_format("date", "yyyyMMdd").cast("int"))
        # Descriptive attributes
        .withColumn("year", year("date"))
        .withColumn("month", month("date"))
        .withColumn("day", dayofmonth("date"))
    )

@dlt.table(
    name="fact_transactions",
    comment="""
    Sales fact table.

    Grain:
      - One row represents one product sold on one order
        (order line item).

    Business Event:
      - A sale occurs when a customer purchases a product.

    Why this table matters:
      - Primary source of revenue truth
      - Supports revenue audit, inventory analysis, and vendor performance
      - Region is sourced from the ORDER (not customer) to reflect
        where the transaction actually occurred
    """
)
def fact_transactions():

    # Orders provide header-level context:
    #   - customer_id
    #   - vendor_id
    #   - region (where the sale occurred)
    #   - order date
    orders = dlt.read_stream("globalmart.silver.orders_silver")

    # Transactions provide line-level metrics:
    #   - product_id
    #   - sales amount
    #   - quantity
    #   - profit
    transactions = dlt.read_stream("globalmart.silver.transactions_silver")

    return (
        orders.alias("o")
        # Join by order_id to enforce the correct grain:
        # one row per order line item
        .join(transactions.alias("t"), "order_id")
    

        .select(
            # Identifiers
            col("o.order_id"),
            col("o.customer_id"),
            col("o.vendor_id"),
            col("t.product_id"),

            # Region comes from the order
            # This ensures correct regional revenue analysis
            col("o.region"),

            # Date handling
            col("o.order_purchase_date").alias("date"),
            date_format(col("o.order_purchase_date"), "yyyyMMdd")
                .cast("int")
                .alias("date_key"),

            # Measures
            col("t.sales").alias("sales_amount"),
            col("t.quantity"),
            col("t.profit")
        )
    )

@dlt.table(
    name="fact_returns",
    comment="""
    Returns fact table.

    Grain:
      - One row per return event.

    Business Event:
      - A customer returns a previously purchased order.

    Business Purpose:
      - Supports return fraud detection
      - Tracks refund amounts
      - Links returns back to original sales
    """
)
def fact_returns():

    returns = dlt.read_stream("globalmart.silver.returns_silver")
    orders = dlt.read_stream("globalmart.silver.orders_silver")

    return (
        returns.alias("r")
        # Join to orders to recover customer, vendor, and region
        .join(orders.alias("o"), "order_id").select(
            col("r.order_id"),
            col("o.customer_id"),
            col("o.vendor_id"),
            col("r.refund_amount"),
            col("r.return_status"),

            # Time attributes
            col("r.return_date").alias("date"),
            date_format(col("r.return_date"), "yyyyMMdd").cast("int").alias("date_key")
        )
    )

@dlt.table(
    name="agg_monthly_revenue",
    comment="""
    Monthly revenue summary by region.

    Supports:
      - Executive dashboards
      - Regional revenue comparisons
      - Audit verification

    This table prevents repeated scanning of fact_transactions.
    """
)
def agg_monthly_revenue():

    fact = dlt.read("fact_transactions")

    return (
        fact
        .withColumn("month", date_format("date", "yyyy-MM"))
        .groupBy("region","month")
        .agg(
            sum("sales_amount").alias("total_sales"),
            countDistinct("order_id").alias("order_count")
        )
    )

@dlt.table(
    name="agg_customer_returns",
    comment="""
    Customer-level return metrics.

    Used to:
      - Identify high-frequency returners
      - Detect cross-region abuse
      - Support fraud investigations
    """
)
def agg_customer_returns():

    returns = dlt.read("fact_returns")

    return (
        returns
        .groupBy("customer_id")
        .agg(
            count("*").alias("return_count"),
            sum("refund_amount").alias("total_refund_amount")
        )
    )

@dlt.table(
    name="agg_vendor_performance",
    comment="""
    Vendor-level return rates.

    Business Question:
      - Which vendors have unusually high return rates?

    Combines sales and returns facts into a business KPI.
    """
)
def agg_vendor_performance():

    sales = dlt.read("fact_transactions")
    returns = dlt.read("fact_returns")

    orders_per_vendor = (
        sales.groupBy("vendor_id")
        .agg(countDistinct("order_id").alias("total_orders"))
    )

    returns_per_vendor = (
        returns.groupBy("vendor_id")
        .agg(count("*").alias("total_returns"))
    )

    return (
        orders_per_vendor
        .join(returns_per_vendor, "vendor_id", "left")
        .fillna(0)
        .withColumn("return_rate", col("total_returns") / col("total_orders"))
    )


@dlt.table(
    name="agg_slow_moving_products",
    comment="""
    Identifies products with low sales volume by region.

    Business Purpose:
      - Detect overstock risk
      - Support inventory rebalancing
      - Identify underperforming products early
    """
)
def agg_slow_moving_products():

    fact = dlt.read("fact_transactions")

    return (
        fact
        .groupBy("product_id", "region")
        .agg(sum("quantity").alias("total_quantity_sold"))
        .filter(col("total_quantity_sold") < 10)
    )
