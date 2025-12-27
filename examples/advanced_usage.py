"""Advanced usage examples for Delta Platform."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, current_date
from delta_platform import DeltaPlatform, PlatformConfig


def streaming_ingestion_example():
    """Example of streaming data ingestion."""
    config = PlatformConfig.create_default("/tmp/delta_platform")

    with DeltaPlatform(config) as platform:
        # Start a streaming ingestion
        query = platform.bronze.ingest_streaming(
            source_path="/data/streaming/events",
            table_name="events_stream",
            format="json",
            trigger_interval="10 seconds"
        )

        # Let it run for demonstration
        query.awaitTermination(timeout=60)
        query.stop()


def scd_type2_example():
    """Example of Slowly Changing Dimension Type 2."""
    config = PlatformConfig.create_default("/tmp/delta_platform")

    with DeltaPlatform(config) as platform:
        # Read source data
        source_df = platform.spark.read.json("/data/customer_updates.json")

        # Apply SCD Type 2
        platform.silver.apply_scd_type2(
            source_df=source_df,
            target_table="customers_scd",
            natural_keys=["customer_id"],
            start_date_col="valid_from",
            end_date_col="valid_to",
            current_flag_col="is_current"
        )


def merge_updates_example():
    """Example of merging updates into a table."""
    config = PlatformConfig.create_default("/tmp/delta_platform")

    with DeltaPlatform(config) as platform:
        # Read updates
        updates_df = platform.spark.read.parquet("/data/product_updates.parquet")

        # Merge updates into silver table
        platform.silver.merge_updates(
            source_df=updates_df,
            target_table="products",
            merge_keys=["product_id"],
            update_columns=["price", "stock", "updated_at"]
        )


def create_star_schema_example():
    """Example of creating a star schema in Gold layer."""
    config = PlatformConfig.create_default("/tmp/delta_platform")

    with DeltaPlatform(config) as platform:
        # Create dimension tables
        platform.gold.create_dimension(
            source_table=platform.silver.table_path("customers_clean"),
            dimension_table="dim_customer",
            dimension_columns=["customer_id", "name", "region", "segment"],
            surrogate_key="customer_key"
        )

        platform.gold.create_dimension(
            source_table=platform.silver.table_path("products_clean"),
            dimension_table="dim_product",
            dimension_columns=["product_id", "product_name", "category", "brand"],
            surrogate_key="product_key"
        )

        # Create fact table
        def join_fact_tables(source_dfs):
            """Join multiple sources for fact table."""
            orders = source_dfs["orders"]
            customers = source_dfs["customers"]
            products = source_dfs["products"]

            return orders \
                .join(customers, "customer_id") \
                .join(products, "product_id")

        platform.gold.create_fact_table(
            source_tables={
                "orders": platform.silver.table_path("orders_clean"),
                "customers": platform.silver.table_path("customers_clean"),
                "products": platform.silver.table_path("products_clean")
            },
            fact_table="fact_sales",
            join_func=join_fact_tables,
            measures=["quantity", "amount", "discount"],
            dimensions=["customer_id", "product_id", "order_date"]
        )


def time_series_example():
    """Example of creating time series aggregations."""
    config = PlatformConfig.create_default("/tmp/delta_platform")

    with DeltaPlatform(config) as platform:
        # Create daily sales time series
        platform.gold.create_time_series(
            source_table=platform.silver.table_path("sales"),
            time_series_table="daily_sales",
            timestamp_column="sale_timestamp",
            group_by=["region", "product_category"],
            metrics={
                "amount": "sum",
                "quantity": "sum",
                "order_id": "count"
            },
            time_grain="day"
        )


def main():
    """Run all examples."""
    print("Running streaming ingestion example...")
    streaming_ingestion_example()

    print("\nRunning SCD Type 2 example...")
    scd_type2_example()

    print("\nRunning merge updates example...")
    merge_updates_example()

    print("\nRunning star schema example...")
    create_star_schema_example()

    print("\nRunning time series example...")
    time_series_example()


if __name__ == "__main__":
    main()
