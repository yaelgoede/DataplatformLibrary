"""
Example: Complete medallion architecture pipeline using table-centric design.

This example shows a full Bronze -> Silver -> Gold data pipeline for an e-commerce platform.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, to_date
from delta_platform import (
    TableBuilder,
    MedallionLayer,
    JsonDataSource,
    ParquetDataSource,
    CleaningStrategy,
    DeduplicationStrategy,
    AggregationStrategy,
    CustomStrategy,
    FilterStrategy
)


def create_spark_session():
    """Create Spark session with Delta Lake support."""
    return SparkSession.builder \
        .appName("MedallionPipelineExample") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


class EcommercePipeline:
    """E-commerce data pipeline following medallion architecture."""

    def __init__(self, spark: SparkSession, base_path: str = "/tmp/ecommerce"):
        """Initialize the pipeline.

        Args:
            spark: Active SparkSession
            base_path: Base path for all tables
        """
        self.spark = spark
        self.base_path = base_path

    def bronze_layer(self):
        """Bronze layer: Ingest raw data from multiple sources."""
        print("\n" + "=" * 60)
        print("BRONZE LAYER: Raw Data Ingestion")
        print("=" * 60)

        # 1. Ingest customer data from JSON
        customers_bronze = (TableBuilder(self.spark)
            .name("customers_bronze")
            .path(f"{self.base_path}/bronze/customers")
            .layer(MedallionLayer.BRONZE)
            .description("Raw customer data from CRM system")
            .build())

        customer_source = JsonDataSource(self.spark, "/data/raw/customers.json")
        customers_df = customer_source.read(multiline=True)
        customers_bronze.append(customers_df, add_metadata=True)

        print(f"✓ Ingested {customers_bronze.count()} customer records")

        # 2. Ingest order data from Parquet
        orders_bronze = (TableBuilder(self.spark)
            .name("orders_bronze")
            .path(f"{self.base_path}/bronze/orders")
            .layer(MedallionLayer.BRONZE)
            .partition_by(["order_date"])
            .description("Raw order data from order management system")
            .build())

        orders_source = ParquetDataSource(self.spark, "/data/raw/orders/*.parquet")
        orders_df = orders_source.read()
        orders_bronze.append(orders_df, add_metadata=True)

        print(f"✓ Ingested {orders_bronze.count()} order records")

        # 3. Ingest product data from JSON
        products_bronze = (TableBuilder(self.spark)
            .name("products_bronze")
            .path(f"{self.base_path}/bronze/products")
            .layer(MedallionLayer.BRONZE)
            .description("Raw product catalog")
            .build())

        products_source = JsonDataSource(self.spark, "/data/raw/products.json")
        products_df = products_source.read()
        products_bronze.append(products_df, add_metadata=True)

        print(f"✓ Ingested {products_bronze.count()} product records")

        return customers_bronze, orders_bronze, products_bronze

    def silver_layer(self, customers_bronze, orders_bronze, products_bronze):
        """Silver layer: Clean, validate, and conform data."""
        print("\n" + "=" * 60)
        print("SILVER LAYER: Data Cleaning and Transformation")
        print("=" * 60)

        # 1. Clean customer data
        customers_silver = (TableBuilder(self.spark)
            .name("customers_silver")
            .path(f"{self.base_path}/silver/customers")
            .layer(MedallionLayer.SILVER)
            .z_order_by(["customer_id"])
            .description("Cleaned and deduplicated customer data")
            .build())

        # Strategy: Clean + deduplicate customers
        customer_cleaning = CleaningStrategy(
            drop_duplicates=False,  # We'll use custom deduplication
            drop_null_columns=["customer_id", "email"],
            trim_strings=True
        )

        customer_dedupe = DeduplicationStrategy(
            dedupe_columns=["customer_id"],
            order_by="_bronze_ingestion_timestamp",
            keep="last"
        )

        # Apply cleaning then deduplication
        cleaned_customers = customers_bronze.transform(customer_cleaning)
        customers_silver.append(
            customer_dedupe.transform(cleaned_customers),
            add_metadata=True
        )

        print(f"✓ Cleaned {customers_silver.count()} customer records")

        # 2. Transform orders with business logic
        orders_silver = (TableBuilder(self.spark)
            .name("orders_silver")
            .path(f"{self.base_path}/silver/orders")
            .layer(MedallionLayer.SILVER)
            .partition_by(["order_date"])
            .z_order_by(["customer_id", "order_id"])
            .description("Validated orders with computed fields")
            .build())

        def enrich_orders(df):
            """Custom transformation for orders."""
            from pyspark.sql.functions import when, col, round

            return df \
                .filter(col("order_status") != "cancelled") \
                .withColumn("total_with_tax", round(col("total") * 1.08, 2)) \
                .withColumn("order_size",
                    when(col("total") < 50, "small")
                    .when(col("total") < 200, "medium")
                    .otherwise("large")
                )

        orders_transformation = CustomStrategy(enrich_orders)
        orders_silver_df = orders_bronze.transform(
            orders_transformation,
            target_table=orders_silver
        )

        print(f"✓ Transformed {orders_silver.count()} order records")

        # 3. Clean product data
        products_silver = (TableBuilder(self.spark)
            .name("products_silver")
            .path(f"{self.base_path}/silver/products")
            .layer(MedallionLayer.SILVER)
            .z_order_by(["product_id"])
            .description("Cleaned product catalog")
            .build())

        products_cleaning = CleaningStrategy(
            drop_duplicates=True,
            drop_null_columns=["product_id", "name"],
            trim_strings=True
        )

        products_bronze.transform(products_cleaning, target_table=products_silver)

        print(f"✓ Cleaned {products_silver.count()} product records")

        return customers_silver, orders_silver, products_silver

    def gold_layer(self, customers_silver, orders_silver, products_silver):
        """Gold layer: Create business-ready aggregations and analytics."""
        print("\n" + "=" * 60)
        print("GOLD LAYER: Business Analytics")
        print("=" * 60)

        # 1. Customer lifetime value
        customer_ltv = (TableBuilder(self.spark)
            .name("customer_ltv")
            .path(f"{self.base_path}/gold/customer_ltv")
            .layer(MedallionLayer.GOLD)
            .description("Customer lifetime value metrics")
            .build())

        ltv_aggregation = AggregationStrategy(
            group_by=["customer_id"],
            aggregations={
                "order_id": "count",
                "total": "sum",
                "total": "avg"
            }
        )

        orders_df = orders_silver.read()
        ltv_df = ltv_aggregation.transform(orders_df)

        # Rename columns for clarity
        ltv_df = ltv_df \
            .withColumnRenamed("order_id_count", "total_orders") \
            .withColumnRenamed("total_sum", "lifetime_value") \
            .withColumnRenamed("total_avg", "average_order_value")

        customer_ltv.overwrite(ltv_df, add_metadata=True)

        print(f"✓ Created customer LTV for {customer_ltv.count()} customers")

        # 2. Daily sales summary
        daily_sales = (TableBuilder(self.spark)
            .name("daily_sales")
            .path(f"{self.base_path}/gold/daily_sales")
            .layer(MedallionLayer.GOLD)
            .partition_by(["order_date"])
            .description("Daily sales aggregations")
            .build())

        daily_aggregation = AggregationStrategy(
            group_by=["order_date"],
            aggregations={
                "order_id": "count",
                "total": "sum",
                "total": "avg"
            }
        )

        daily_df = daily_aggregation.transform(orders_df)
        daily_df = daily_df \
            .withColumnRenamed("order_id_count", "order_count") \
            .withColumnRenamed("total_sum", "total_revenue") \
            .withColumnRenamed("total_avg", "average_order_value")

        daily_sales.overwrite(daily_df, add_metadata=True)

        print(f"✓ Created daily sales summary for {daily_sales.count()} days")

        # 3. Product performance
        product_performance = (TableBuilder(self.spark)
            .name("product_performance")
            .path(f"{self.base_path}/gold/product_performance")
            .layer(MedallionLayer.GOLD)
            .description("Product sales performance metrics")
            .build())

        # Custom aggregation for product performance
        def calculate_product_metrics(df):
            """Calculate product performance metrics."""
            from pyspark.sql.functions import sum, count, avg

            return df.groupBy("product_id").agg(
                count("order_id").alias("times_ordered"),
                sum("quantity").alias("total_quantity_sold"),
                sum("total").alias("total_revenue"),
                avg("price").alias("average_price")
            )

        # Read order items (assuming it's in orders data)
        # This would typically join orders with order_items
        # For this example, we'll use a simplified version
        product_strategy = CustomStrategy(calculate_product_metrics)
        product_perf_df = orders_silver.transform(product_strategy)

        product_performance.overwrite(product_perf_df, add_metadata=True)

        print(f"✓ Created product performance for {product_performance.count()} products")

        # 4. Create a wide denormalized table for BI tools
        customer_orders_wide = (TableBuilder(self.spark)
            .name("customer_orders_wide")
            .path(f"{self.base_path}/gold/customer_orders_wide")
            .layer(MedallionLayer.GOLD)
            .description("Denormalized customer-order data for BI reporting")
            .build())

        # Join customers, orders, and LTV
        customers_df = customers_silver.read()
        ltv_df = customer_ltv.read()

        wide_df = orders_df \
            .join(customers_df, "customer_id", "left") \
            .join(ltv_df, "customer_id", "left") \
            .select(
                "order_id",
                "customer_id",
                customers_df["name"].alias("customer_name"),
                customers_df["email"].alias("customer_email"),
                customers_df["country"].alias("customer_country"),
                "order_date",
                "order_status",
                "order_size",
                "total",
                "total_with_tax",
                "lifetime_value",
                "total_orders"
            )

        customer_orders_wide.overwrite(wide_df, add_metadata=True)

        print(f"✓ Created denormalized table with {customer_orders_wide.count()} records")

        return customer_ltv, daily_sales, product_performance, customer_orders_wide

    def optimize_all_tables(self):
        """Optimize all tables in the pipeline."""
        print("\n" + "=" * 60)
        print("TABLE OPTIMIZATION")
        print("=" * 60)

        # List all tables to optimize
        tables_to_optimize = [
            (f"{self.base_path}/bronze/customers", ["customer_id"]),
            (f"{self.base_path}/bronze/orders", ["order_id"]),
            (f"{self.base_path}/silver/customers", ["customer_id"]),
            (f"{self.base_path}/silver/orders", ["customer_id", "order_id"]),
            (f"{self.base_path}/gold/customer_ltv", None),
            (f"{self.base_path}/gold/daily_sales", None),
        ]

        for path, z_order in tables_to_optimize:
            try:
                # Create a temporary table instance for optimization
                temp_table = (TableBuilder(self.spark)
                    .name("temp")
                    .path(path)
                    .layer(MedallionLayer.BRONZE)  # Layer doesn't matter for optimization
                    .build())

                if temp_table.exists:
                    temp_table.optimize(z_order_by=z_order)
                    print(f"✓ Optimized {path}")
            except Exception as e:
                print(f"✗ Failed to optimize {path}: {e}")

    def run_pipeline(self):
        """Run the complete medallion pipeline."""
        print("\n" + "🚀 Starting E-commerce Medallion Pipeline")
        print("=" * 60)

        # Bronze layer
        customers_bronze, orders_bronze, products_bronze = self.bronze_layer()

        # Silver layer
        customers_silver, orders_silver, products_silver = self.silver_layer(
            customers_bronze, orders_bronze, products_bronze
        )

        # Gold layer
        customer_ltv, daily_sales, product_performance, customer_orders_wide = self.gold_layer(
            customers_silver, orders_silver, products_silver
        )

        # Optimization
        self.optimize_all_tables()

        print("\n" + "=" * 60)
        print("✅ Pipeline completed successfully!")
        print("=" * 60)

        # Print summary
        print("\nPipeline Summary:")
        print(f"  Bronze Tables: 3 (customers, orders, products)")
        print(f"  Silver Tables: 3 (customers, orders, products)")
        print(f"  Gold Tables: 4 (customer_ltv, daily_sales, product_performance, customer_orders_wide)")
        print(f"\nTotal records processed:")
        print(f"  Customers: {customers_silver.count()}")
        print(f"  Orders: {orders_silver.count()}")
        print(f"  Products: {products_silver.count()}")


def main():
    """Run the medallion pipeline."""
    spark = create_spark_session()

    try:
        pipeline = EcommercePipeline(spark)
        pipeline.run_pipeline()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
