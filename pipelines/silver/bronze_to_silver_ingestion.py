import dlt
from pyspark.sql.functions import *


# ============================================================
# Helper: Standardizes all column names into lower_snake_case
# ============================================================
def normalize_columns(df):
    """
    Normalize column names across all 6 regional customer files.
    This eliminates differences like:
      - 'CustomerID'
      - 'cust_id'
      - 'customer identifier'
    """
    return df.toDF(*[c.strip().lower().replace(" ", "_") for c in df.columns])


# ============================================================
# Helper: Safely select the first matching column name
# ============================================================
def get_col(df, *names):
    """
    Some regions use different column names for the same field.
    Example:
      Region 1 → customer_id
      Region 2 → CustomerID
      Region 3 → cust_id
    This helper returns the *first* matching column from a list,
    or NULL if none exist.
    """
    for n in names:
        if n in df.columns:
            return col(n)
    return lit(None)


# ============================================================
# ✅ SILVER TRANSFORMATION LAYER (Base View)
# ------------------------------------------------------------
# This view:
#   - Reads streaming bronze table
#   - Normalizes schema
#   - Applies ALL cleaning & quality rules
#   - Adds _is_valid and _dq_reason flags
#   - Does NOT write out tables (clean/quarantine split happens later)
# ============================================================
@dlt.view(
    name="customers_silver_base",
    comment="Applies all Silver-level standardization & data quality rules for customers."
)
def customers_silver_base():

    # ✅ Streaming read from Bronze because Bronze was created with Auto Loader
    df = dlt.read_stream("globalmart.bronze.customers")

    # ✅ Normalize column names so all regions match
    df = normalize_columns(df)

    # ✅ Standardize schema and business fields
    df = (
        df
        # Harmonize all variations of customer_id
        .withColumn("customer_id", get_col(df, "customer_id", "customerid", "cust_id"))

        # Harmonize email column
        .withColumn("customer_email", get_col(df, "customer_email", "email", "email_address"))

        # Harmonize names
        .withColumn("customer_name", get_col(df, "customer_name", "name", "full_name"))

        # Normalize segment values (example: "CONS" → "Consumer")
        .withColumn(
            "segment",
            when(col("segment") == "CONS", "Consumer").otherwise(col("segment"))
        )
    )

    # ============================================================
    # ✅ SILVER DATA QUALITY RULES
    # ============================================================

    df = (
        df
        # Rule 1: customer_id must be present
        .withColumn("_dq_id_ok", col("customer_id").isNotNull())

        # Rule 2: customer_email must be present
        .withColumn("_dq_email_ok", col("customer_email").isNotNull())

        # Rule 3: segment must be one of 3 valid values
        .withColumn("_dq_segment_ok",
            col("segment").isin("Consumer", "Corporate", "Home Office")
        )

        # ✅ Final valid flag (record must pass ALL rules)
        .withColumn(
            "_is_valid",
            col("_dq_id_ok") &
            col("_dq_email_ok") &
            col("_dq_segment_ok")
        )

        # ✅ Reason for failure (visible to finance team)
        .withColumn(
            "_dq_reason",
            when(~col("_dq_id_ok"), "Missing customer_id")
            .when(~col("_dq_email_ok"), "Missing customer_email")
            .when(~col("_dq_segment_ok"), "Invalid segment value")
        )
    )

    # Return the base cleaned dataset (not split yet)
    return df


# ============================================================
# ✅ CLEAN SILVER TABLE
# ------------------------------------------------------------
# This table contains only valid, business-ready records.
# It's what downstream Gold tables will read from.
# ============================================================
@dlt.table(
    name="customers_silver",
    comment="Validated and standardized customer records for analytics."
)
def customers_silver():
    return dlt.read_stream("customers_silver_base").filter(col("_is_valid") == True)


# ============================================================
# ✅ QUARANTINE TABLE
# ------------------------------------------------------------
# Contains bad records with the rejection reason.
# This is what Finance reviews when values fail DQ rules.
# ============================================================
@dlt.table(
    name="customers_quarantine",
    comment="Records that failed customer DQ checks with reasons for finance review."
)
def customers_quarantine():
    return dlt.read_stream("customers_silver_base").filter(col("_is_valid") == False)


# ====================================================================================

       ############### ORDERS ##################

# ====================================================================================


@dlt.view(
    name="orders_silver_base",
    comment="""
    Silver base view for orders.

    FIX APPLIED:
    ------------
    - Region is added to orders by joining with customers_silver.
    - Region becomes an order-level attribute, frozen at order time.
    - This enables correct regional revenue, inventory, and fraud analysis
      in the Gold layer.

    Design Rationale:
    -----------------
    Customers may change regions over time, but orders represent a
    point-in-time business event. Capturing region at order creation
    preserves historical correctness.
    """
)
def orders_silver_base():

    # ------------------------------------------------------------
    # Read Bronze orders as a streaming source
    # ------------------------------------------------------------
    orders = dlt.read_stream("globalmart.bronze.orders")
    orders = normalize_columns(orders)

    # ------------------------------------------------------------
    # Read Silver customers (already deduplicated and cleaned)
    # ------------------------------------------------------------
    customers = dlt.read("customers_silver")

    # ------------------------------------------------------------
    # Enrich orders with region from customers
    # ------------------------------------------------------------
    df = (
        orders.alias("o")
        .join(
            customers.alias("c"),
            col("o.customer_id") == col("c.customer_id"),
            "left"
        )
        .select(
            # Identifiers
            col("o.order_id"),
            col("o.customer_id"),
            col("o.vendor_id"),

            # ✅ FIX: region is now frozen at order time
            col("c.region").alias("region"),

            # Order attributes
            col("o.ship_mode"),
            col("o.order_purchase_date")
        )
    )

    # ------------------------------------------------------------
    # Normalize ship_mode values
    # ------------------------------------------------------------
    df = df.withColumn(
        "ship_mode",
        when(col("ship_mode").isin("1st Class", "First Class"), "First Class")
        .otherwise(col("ship_mode"))
    )

    # ------------------------------------------------------------
    # Normalize date formats
    # ------------------------------------------------------------
    df = df.withColumn(
        "order_purchase_date",
        coalesce(
            to_timestamp(col("order_purchase_date"), "MM/dd/yyyy HH:mm"),
            to_timestamp(col("order_purchase_date"), "yyyy-MM-dd HH:mm")
        )
    )

    # ------------------------------------------------------------
    # Data Quality Rules
    # ------------------------------------------------------------
    df = (
        df
        .withColumn("_dq_order", col("order_id").isNotNull())
        .withColumn("_dq_customer", col("customer_id").isNotNull())
        .withColumn("_dq_vendor", col("vendor_id").isNotNull())
        .withColumn("_dq_region", col("region").isNotNull())

        .withColumn(
            "_is_valid",
            col("_dq_order") &
            col("_dq_customer") &
            col("_dq_vendor") &
            col("_dq_region")
        )

        .withColumn(
            "_dq_reason",
            when(~col("_dq_order"), "Missing order_id")
            .when(~col("_dq_customer"), "Missing customer_id")
            .when(~col("_dq_vendor"), "Missing vendor_id")
            .when(~col("_dq_region"), "Missing region")
        )
    )

    return df

@dlt.table(name="orders_silver")
def orders_silver():
    return dlt.read_stream("orders_silver_base").filter(col("_is_valid"))


@dlt.table(name="orders_quarantine")
def orders_quarantine():
    return dlt.read_stream("orders_silver_base").filter(~col("_is_valid"))



# =========================================================================

            #########TRANSACTIONS############
# =========================================================================


@dlt.view(
    name="transactions_silver_base",
    comment="Silver base: DQ + standardization for transactions data."
)
def transactions_silver_base():

    df = dlt.read_stream("globalmart.bronze.transactions")
    df = normalize_columns(df)

    df = (
        df
        .withColumn("order_id", get_col(df, "order_id", "orderid"))
        .withColumn("product_id", get_col(df, "product_id", "productid"))

        # Normalize discount ("40%" → 0.40)
        .withColumn("discount",
            when(col("discount").like("%"), regexp_replace(col("discount"), "%", "").cast("double")/100)
            .otherwise(col("discount").cast("double"))
        )

        .withColumn("quantity", col("quantity").cast("int"))
        .withColumn("sales", col("sales").cast("double"))
        .withColumn("profit", col("profit").cast("double"))

        # ----------------------------
        # ✅ Data Quality Rules
        # ----------------------------
        .withColumn("_dq_order", col("order_id").isNotNull())
        .withColumn("_dq_product", col("product_id").isNotNull())
        .withColumn("_dq_qty", col("quantity") > 0)

        .withColumn("_is_valid",
            col("_dq_order") &
            col("_dq_product") &
            col("_dq_qty")
        )

        .withColumn("_dq_reason",
            when(~col("_dq_order"), "Missing order_id")
            .when(~col("_dq_product"), "Missing product_id")
            .when(~col("_dq_qty"), "Invalid quantity")
        )
    )

    return df

@dlt.table(name="transactions_silver")
def transactions_silver():
    return dlt.read_stream("transactions_silver_base").filter(col("_is_valid"))

@dlt.table(name="transactions_quarantine")
def transactions_quarantine():
    return dlt.read_stream("transactions_silver_base").filter(~col("_is_valid"))

# =====================================================================

    ###############RETURNS################
# =====================================================================

@dlt.view(
    name="returns_silver_base",
    comment="Silver base: cleans returns data + applies DQ rules."
)
def returns_silver_base():

    df = dlt.read_stream("globalmart.bronze.returns")
    df = normalize_columns(df)

    df = (
        df
        .withColumn("order_id", get_col(df, "order_id", "orderid"))

        # refund amount cleanup
        .withColumn("refund_amount",
            regexp_replace(get_col(df, "refund_amount", "amount"), "[$]", "").cast("double")
        )

        # normalize return status
        .withColumn("return_status_raw", upper(get_col(df, "return_status", "status")))
        .withColumn("return_status",
            when(col("return_status_raw").isin("REJECTED","RJCTD"), "Rejected")
            .when(col("return_status_raw").isin("APPROVED","APPRVD"), "Approved")
            .when(col("return_status_raw").isin("PENDING","PENDG"), "Pending")
            .otherwise("Unknown")
        )

        # parse multiple date formats
        .withColumn("return_date",
            coalesce(
                to_date(get_col(df, "date_of_return", "return_date"), "yyyy-MM-dd"),
                to_date(get_col(df, "date_of_return", "return_date"), "MM/dd/yyyy")
            )
        )

        # ----------------------------
        # ✅ Data Quality Rules
        # ----------------------------
        .withColumn("_dq_order", col("order_id").isNotNull())
        .withColumn("_dq_refund", col("refund_amount").isNotNull())

        .withColumn("_is_valid",
            col("_dq_order") & col("_dq_refund")
        )

        .withColumn("_dq_reason",
            when(~col("_dq_order"), "Missing order_id")
            .when(~col("_dq_refund"), "Missing refund_amount")
        )
    )

    return df


@dlt.table(name="returns_silver")
def returns_silver():
    return dlt.read_stream("returns_silver_base").filter(col("_is_valid"))

@dlt.table(name="returns_quarantine")
def returns_quarantine():
    return dlt.read_stream("returns_silver_base").filter(~col("_is_valid"))



# ========================================================================

          ######## PRODUCTS #########
# ========================================================================

@dlt.view(
    name="products_silver_base",
    comment="Silver base: cleans and validates product data."
)
def products_silver_base():

    df = dlt.read_stream("globalmart.bronze.products")
    df = normalize_columns(df)

    df = (
        df
        .withColumn("product_id", get_col(df, "product_id", "productid"))

        # upc normalization (remove scientific notation)
        .withColumn("upc",
            format_string("%.0f", col("upc").cast("double"))
        )

        # DQ rule
        .withColumn("_dq_product", col("product_id").isNotNull())

        .withColumn("_is_valid", col("_dq_product"))

        .withColumn(
            "_dq_reason",
            when(~col("_dq_product"), "Missing product_id")
        )
    )

    return df

@dlt.table(name="products_silver")
def products_silver():
    return dlt.read_stream("products_silver_base").filter(col("_is_valid"))

@dlt.table(name="products_quarantine")
def products_quarantine():
    return dlt.read_stream("products_silver_base").filter(~col("_is_valid"))

# ====================================================
    
    ##############VENDORS######

# ====================================================


@dlt.view(
    name="vendors_silver_base",
    comment="Silver base: cleans vendor data and validates vendor IDs."
)
def vendors_silver_base():

    df = dlt.read_stream("globalmart.bronze.vendors")
    df = normalize_columns(df)

    df = (
        df
        .withColumn("vendor_id", get_col(df,"vendor_id","vendorid"))
        .withColumn("vendor_name", initcap(col("vendor_name")))

        # DQ rule
        .withColumn("_dq_vendor", col("vendor_id").isNotNull())
        .withColumn("_is_valid", col("_dq_vendor"))
        .withColumn(
            "_dq_reason",
            when(~col("_dq_vendor"), "Missing vendor_id")
        )
    )

    return df

@dlt.table(name="vendors_silver")
def vendors_silver():
    return dlt.read_stream("vendors_silver_base").filter(col("_is_valid"))


@dlt.table(name="vendors_quarantine")
def vendors_quarantine():
    return dlt.read_stream("vendors_silver_base").filter(~col("_is_valid"))
