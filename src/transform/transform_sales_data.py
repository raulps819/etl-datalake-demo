"""
PySpark ETL transformation module for sales data.
Cleans data quality issues and creates star schema for analytics.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
import sys
import os


def create_spark_session(app_name="SalesDataETL"):
    """Create and configure Spark session"""
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def clean_customers(spark_df):
    """
    Clean customer data using PySpark.
    
    - Remove invalid emails
    - Handle missing values  
    - Remove duplicates
    """
    print("Cleaning customers data...")
    initial_count = spark_df.count()
    
    # Remove records with missing emails
    cleaned = spark_df.filter(F.col("email").isNotNull())
    
    # Validate email format (basic regex)
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    cleaned = cleaned.filter(F.col("email").rlike(email_pattern))
    
    # Remove duplicate emails (keep first)
    window_spec = Window.partitionBy("email").orderBy("customer_id")
    cleaned = cleaned.withColumn("row_num", F.row_number().over(window_spec))
    cleaned = cleaned.filter(F.col("row_num") == 1).drop("row_num")
    
    # Fill missing values
    cleaned = cleaned.fillna({
        "phone": "Unknown",
        "city": "Unknown"
    })
    
    # Ensure proper date type
    cleaned = cleaned.withColumn(
        "registration_date", 
        F.to_date(F.col("registration_date"))
    )
    
    final_count = cleaned.count()
    print(f"  Removed {initial_count - final_count} invalid customer records")
    print(f"  Remaining: {final_count} customers")
    
    return cleaned


def clean_products(spark_df):
    """
    Clean product data using PySpark.
    
    - Fix negative prices and stock
    - Handle missing values
    - Validate ratings
    """
    print("Cleaning products data...")
    initial_count = spark_df.count()
    
    # Remove products with non-positive prices
    cleaned = spark_df.filter(F.col("unit_price") > 0)
    
    # Fix negative stock (set to 0)
    cleaned = cleaned.withColumn(
        "stock_quantity",
        F.when(F.col("stock_quantity") < 0, 0)
         .otherwise(F.col("stock_quantity"))
    )
    
    # Fill missing supplier
    cleaned = cleaned.fillna({"supplier": "Unknown"})
    
    # Fill missing weight with category median
    weight_medians = cleaned.groupBy("category") \
        .agg(F.percentile_approx("weight_kg", 0.5).alias("median_weight"))
    
    cleaned = cleaned.join(weight_medians, "category", "left")
    cleaned = cleaned.withColumn(
        "weight_kg",
        F.when(F.col("weight_kg").isNull(), F.col("median_weight"))
         .otherwise(F.col("weight_kg"))
    ).drop("median_weight")
    
    # Fix invalid ratings (cap between 0 and 5)
    cleaned = cleaned.withColumn(
        "rating",
        F.when(F.col("rating") > 5.0, 5.0)
         .when(F.col("rating") < 0.0, 0.0)
         .otherwise(F.col("rating"))
    )
    
    final_count = cleaned.count()
    print(f"  Removed {initial_count - final_count} invalid product records")
    print(f"  Remaining: {final_count} products")
    
    return cleaned


def clean_sales(spark_df, valid_customers_df, valid_products_df):
    """
    Clean sales data using PySpark.
    
    - Remove orphaned foreign keys
    - Fix negative quantities
    - Handle missing/invalid dates
    - Fix invalid discounts
    - Remove duplicates
    """
    print("Cleaning sales data...")
    initial_count = spark_df.count()
    
    # Remove sales with missing dates
    cleaned = spark_df.filter(F.col("date").isNotNull())
    
    # Convert to date type
    cleaned = cleaned.withColumn("date", F.to_date(F.col("date")))
    
    # Remove future dates
    cleaned = cleaned.filter(F.col("date") <= F.current_date())
    
    # Remove orphaned customer_ids (inner join keeps only valid)
    valid_customer_ids = valid_customers_df.select("customer_id")
    cleaned = cleaned.join(
        valid_customer_ids, 
        "customer_id", 
        "inner"
    )
    
    # Remove orphaned product_ids
    valid_product_ids = valid_products_df.select("product_id")
    cleaned = cleaned.join(
        valid_product_ids,
        "product_id",
        "inner"
    )
    
    # Remove negative quantities
    cleaned = cleaned.filter(F.col("quantity") > 0)
    
    # Fix invalid discounts (cap at 100%)
    cleaned = cleaned.withColumn(
        "discount_percent",
        F.when(F.col("discount_percent") > 100, 100)
         .when(F.col("discount_percent") < 0, 0)
         .otherwise(F.col("discount_percent"))
    )
    
    # Remove duplicate sale_ids (keep first)
    window_spec = Window.partitionBy("sale_id").orderBy("date")
    cleaned = cleaned.withColumn("row_num", F.row_number().over(window_spec))
    cleaned = cleaned.filter(F.col("row_num") == 1).drop("row_num")
    
    final_count = cleaned.count()
    print(f"  Removed {initial_count - final_count} invalid sales records")
    print(f"  Remaining: {final_count} sales")
    
    return cleaned


def create_star_schema(customers_df, products_df, sales_df):
    """
    Create star schema with fact and dimension tables.
    
    Returns:
        Tuple of (fact_sales, dim_customer, dim_product)
    """
    print("Creating star schema...")
    
    # Dimension: Customers
    dim_customer = customers_df.select(
        "customer_id",
        "name",
        "email",
        "country",
        "registration_date",
        "segment",
        "city"
    )
    
    # Dimension: Products
    dim_product = products_df.select(
        "product_id",
        "product_name",
        "category",
        "unit_price",
        "supplier",
        "rating"
    )
    
    # Fact: Sales with calculated metrics
    fact_sales = sales_df.join(
        products_df.select("product_id", "unit_price"),
        "product_id",
        "left"
    )
    
    # Calculate amounts
    fact_sales = fact_sales.withColumn(
        "amount_before_discount",
        F.col("quantity") * F.col("unit_price")
    )
    
    fact_sales = fact_sales.withColumn(
        "discount_amount",
        F.col("amount_before_discount") * (F.col("discount_percent") / 100)
    )
    
    fact_sales = fact_sales.withColumn(
        "total_amount",
        F.col("amount_before_discount") - F.col("discount_amount")
    )
    
    # Select final fact table columns and rename date to sale_date
    fact_sales = fact_sales.select(
        "sale_id",
        "customer_id",
        "product_id",
        F.col("date").alias("sale_date"),
        "quantity",
        "unit_price",
        "discount_percent",
        "amount_before_discount",
        "discount_amount",
        "total_amount",
        "payment_method",
        "status"
    )
    
    print(f"  dim_customer: {dim_customer.count()} records")
    print(f"  dim_product: {dim_product.count()} records")
    print(f"  fact_sales: {fact_sales.count()} records")
    
    return fact_sales, dim_customer, dim_product


def main():
    """Run the complete PySpark ETL transformation pipeline"""
    print("=" * 60)
    print("PySpark ETL Transformation Pipeline")
    print("=" * 60)
    
    # Create Spark session
    spark = create_spark_session()
    print(f"Spark version: {spark.version}\n")
    
    # Load raw data
    print("Loading raw data...")
    customers_raw = spark.read.csv(
        "data/raw/customers.csv",
        header=True,
        inferSchema=True
    )
    
    products_raw = spark.read.csv(
        "data/raw/products.csv",
        header=True,
        inferSchema=True
    )
    
    sales_raw = spark.read.csv(
        "data/raw/sales.csv",
        header=True,
        inferSchema=True
    )
    
    print(f"Loaded {customers_raw.count()} customers")
    print(f"Loaded {products_raw.count()} products")
    print(f"Loaded {sales_raw.count()} sales\n")
    
    # Clean data
    print("Cleaning data...")
    customers_clean = clean_customers(customers_raw)
    products_clean = clean_products(products_raw)
    sales_clean = clean_sales(sales_raw, customers_clean, products_clean)
    print()
    
    # Create star schema
    fact_sales, dim_customer, dim_product = create_star_schema(
        customers_clean,
        products_clean,
        sales_clean
    )
    print()
    
    # Save processed data
    print("Saving processed data...")
    
    # Use coalesce to avoid too many small files
    dim_customer.coalesce(1).write.mode("overwrite").csv(
        "data/processed/dim_customer",
        header=True
    )
    
    dim_product.coalesce(1).write.mode("overwrite").csv(
        "data/processed/dim_product",
        header=True
    )
    
    fact_sales.coalesce(4).write.mode("overwrite").csv(
        "data/processed/fact_sales",
        header=True
    )
    
    print("Saved star schema to data/processed/")
    print("=" * 60)
    print("Transformation pipeline completed successfully!")
    print("=" * 60)
    
    spark.stop()


if __name__ == "__main__":
    main()
