"""
Example: Managing individual Delta tables using the new table-centric design.

This example demonstrates how to:
1. Create and configure tables using the Builder pattern
2. Append and manage data in a single table
3. Transform data using Strategy pattern
4. Ingest from various data sources using Repository pattern
"""

from pyspark.sql import SparkSession
from delta_platform import (
    TableBuilder,
    MedallionLayer,
    JsonDataSource,
    ParquetDataSource,
    CleaningStrategy,
    DeduplicationStrategy,
    CustomStrategy,
    FilterStrategy
)


def create_spark_session():
    """Create a Spark session with Delta Lake support."""
    return SparkSession.builder \
        .appName("TableManagementExample") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def example_1_create_and_append_table(spark):
    """Example 1: Create a table and append data."""
    print("=" * 60)
    print("Example 1: Create table and append data")
    print("=" * 60)

    # Build a table using the Builder pattern
    users_table = (TableBuilder(spark)
        .name("users")
        .path("/tmp/delta/bronze/users")
        .layer(MedallionLayer.BRONZE)
        .partition_by(["country"])
        .z_order_by(["user_id"])
        .description("Raw user data from various sources")
        .tag("owner", "data-engineering")
        .tag("pii", "true")
        .build())

    print(f"Created table: {users_table}")

    # Ingest data from JSON source using Repository pattern
    json_source = JsonDataSource(spark, "/data/raw/users/*.json")
    users_df = json_source.read(multiline=True)

    # Append data to the table
    users_table.append(users_df, add_metadata=True, deduplicate=True, deduplicate_columns=["user_id"])

    # Check table statistics
    stats = users_table.statistics()
    print(f"Table statistics: {stats}")

    print()


def example_2_transform_with_strategies(spark):
    """Example 2: Transform data using Strategy pattern."""
    print("=" * 60)
    print("Example 2: Transform data using strategies")
    print("=" * 60)

    # Create source and target tables
    raw_table = (TableBuilder(spark)
        .name("events_raw")
        .path("/tmp/delta/bronze/events")
        .layer(MedallionLayer.BRONZE)
        .build())

    clean_table = (TableBuilder(spark)
        .name("events_clean")
        .path("/tmp/delta/silver/events")
        .layer(MedallionLayer.SILVER)
        .partition_by(["date"])
        .build())

    # Load some data
    events_source = ParquetDataSource(spark, "/data/raw/events/*.parquet")
    events_df = events_source.read()
    raw_table.append(events_df)

    # Apply cleaning strategy
    cleaning = CleaningStrategy(
        drop_duplicates=True,
        drop_null_columns=["event_id", "user_id"],
        trim_strings=True
    )

    # Transform from bronze to silver
    cleaned_df = raw_table.transform(cleaning, target_table=clean_table)

    print(f"Transformed {cleaned_df.count()} records to clean table")
    print()


def example_3_deduplication_strategy(spark):
    """Example 3: Deduplicate data keeping latest records."""
    print("=" * 60)
    print("Example 3: Deduplication strategy")
    print("=" * 60)

    table = (TableBuilder(spark)
        .name("transactions")
        .path("/tmp/delta/bronze/transactions")
        .layer(MedallionLayer.BRONZE)
        .build())

    # Append data (possibly with duplicates)
    transactions_source = JsonDataSource(spark, "/data/raw/transactions/*.json")
    transactions_df = transactions_source.read()
    table.append(transactions_df)

    # Create deduplication strategy - keep latest record per transaction_id
    dedupe_strategy = DeduplicationStrategy(
        dedupe_columns=["transaction_id"],
        order_by="timestamp",
        ascending=False,  # Keep the latest
        keep="last"
    )

    # Apply deduplication
    dedupe_table = (TableBuilder(spark)
        .name("transactions_dedupe")
        .path("/tmp/delta/silver/transactions_dedupe")
        .layer(MedallionLayer.SILVER)
        .build())

    deduped_df = table.transform(dedupe_strategy, target_table=dedupe_table)

    print(f"Deduplicated to {deduped_df.count()} unique records")
    print()


def example_4_custom_transformation(spark):
    """Example 4: Custom transformation logic."""
    print("=" * 60)
    print("Example 4: Custom transformation")
    print("=" * 60)

    def my_custom_transform(df, min_amount=0):
        """Custom transformation: filter and enrich."""
        from pyspark.sql.functions import when, col, lit

        return df \
            .filter(col("amount") > min_amount) \
            .withColumn("amount_category",
                when(col("amount") < 100, lit("small"))
                .when(col("amount") < 1000, lit("medium"))
                .otherwise(lit("large"))
            )

    source_table = (TableBuilder(spark)
        .name("orders")
        .path("/tmp/delta/bronze/orders")
        .layer(MedallionLayer.BRONZE)
        .build())

    # Apply custom transformation
    custom_strategy = CustomStrategy(my_custom_transform)

    enriched_table = (TableBuilder(spark)
        .name("orders_enriched")
        .path("/tmp/delta/silver/orders_enriched")
        .layer(MedallionLayer.SILVER)
        .build())

    enriched_df = source_table.transform(
        custom_strategy,
        target_table=enriched_table,
        min_amount=50  # Pass parameters to custom function
    )

    print(f"Applied custom transformation to {enriched_df.count()} records")
    print()


def example_5_merge_updates(spark):
    """Example 5: Merge updates into a table."""
    print("=" * 60)
    print("Example 5: Merge (upsert) updates")
    print("=" * 60)

    # Create a products table
    products_table = (TableBuilder(spark)
        .name("products")
        .path("/tmp/delta/silver/products")
        .layer(MedallionLayer.SILVER)
        .build())

    # Initial load
    initial_data = spark.createDataFrame([
        (1, "Widget A", 10.99, 100),
        (2, "Widget B", 20.99, 50),
        (3, "Widget C", 15.99, 75),
    ], ["product_id", "name", "price", "stock"])

    products_table.append(initial_data, add_metadata=False)

    # Updates: change prices and stock for existing products, add new product
    updates = spark.createDataFrame([
        (1, "Widget A", 11.99, 95),   # Updated price and stock
        (2, "Widget B", 19.99, 60),   # Updated price and stock
        (4, "Widget D", 25.99, 30),   # New product
    ], ["product_id", "name", "price", "stock"])

    # Merge updates
    products_table.merge(
        source_df=updates,
        merge_keys=["product_id"],
        update_columns=["price", "stock"]  # Only update these columns
    )

    print("Merged updates into products table")
    result_df = products_table.read()
    result_df.show()
    print()


def example_6_table_maintenance(spark):
    """Example 6: Table optimization and maintenance."""
    print("=" * 60)
    print("Example 6: Table maintenance")
    print("=" * 60)

    table = (TableBuilder(spark)
        .name("large_table")
        .path("/tmp/delta/gold/large_table")
        .layer(MedallionLayer.GOLD)
        .z_order_by(["customer_id", "date"])
        .build())

    # Optimize table with Z-ordering
    table.optimize()
    print("Table optimized with Z-ordering on customer_id and date")

    # Vacuum old files (7 days retention)
    table.vacuum(retention_hours=168)
    print("Old files vacuumed (7 days retention)")

    # Get table history
    history = table.history(limit=5)
    print("\nRecent table history:")
    history.select("version", "operation", "timestamp").show()

    print()


def example_7_chaining_operations(spark):
    """Example 7: Chain multiple operations on a table."""
    print("=" * 60)
    print("Example 7: Chaining operations")
    print("=" * 60)

    # Create sample data
    data = spark.createDataFrame([
        (1, "Alice", 100, "USA"),
        (2, "Bob", 200, "UK"),
        (3, "Charlie", 150, "USA"),
        (1, "Alice", 100, "USA"),  # Duplicate
    ], ["user_id", "name", "amount", "country"])

    # Create table and chain operations
    table = (TableBuilder(spark)
        .name("user_data")
        .path("/tmp/delta/silver/user_data")
        .layer(MedallionLayer.SILVER)
        .partition_by(["country"])
        .build())

    # Chain: append -> optimize -> get stats
    stats = (table
        .append(data, deduplicate=True, deduplicate_columns=["user_id"])
        .optimize()
        .statistics())

    print(f"Final table statistics: {stats}")
    print()


def main():
    """Run all examples."""
    spark = create_spark_session()

    try:
        example_1_create_and_append_table(spark)
        example_2_transform_with_strategies(spark)
        example_3_deduplication_strategy(spark)
        example_4_custom_transformation(spark)
        example_5_merge_updates(spark)
        example_6_table_maintenance(spark)
        example_7_chaining_operations(spark)

        print("=" * 60)
        print("All examples completed successfully!")
        print("=" * 60)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
