"""Basic usage example for Delta Platform."""

from pyspark.sql import SparkSession
from delta_platform import DeltaPlatform, PlatformConfig


def main():
    # Create a default configuration
    config = PlatformConfig.create_default("/tmp/delta_platform")

    # Or load from YAML
    # config = PlatformConfig.from_yaml("config.yaml")

    # Initialize the platform
    with DeltaPlatform(config) as platform:
        # Ingest data into Bronze layer
        print("Ingesting data into Bronze layer...")

        # Ingest JSON data
        platform.bronze.ingest_json(
            source_path="/data/raw/users.json",
            table_name="users_raw",
            mode="overwrite"
        )

        # Ingest Parquet data
        platform.bronze.ingest_parquet(
            source_path="/data/raw/transactions.parquet",
            table_name="transactions_raw",
            mode="overwrite"
        )

        # Ingest XML data
        platform.bronze.ingest_xml(
            source_path="/data/raw/products.xml",
            table_name="products_raw",
            row_tag="product",
            mode="overwrite"
        )

        print("Bronze layer ingestion complete!")

        # Transform data to Silver layer
        print("Transforming data to Silver layer...")

        def clean_users(df):
            """Clean and transform users data."""
            from pyspark.sql.functions import trim, upper

            return df.select(
                "user_id",
                trim(upper("name")).alias("name"),
                "email",
                "created_at"
            ).dropna()

        platform.silver.transform_from_bronze(
            bronze_table=platform.bronze.table_path("users_raw"),
            silver_table="users_clean",
            transformation_func=clean_users,
            mode="overwrite",
            deduplicate_columns=["user_id"]
        )

        print("Silver layer transformation complete!")

        # Create Gold layer aggregations
        print("Creating Gold layer aggregations...")

        def aggregate_transactions(source_dfs):
            """Aggregate transaction data."""
            from pyspark.sql.functions import sum, count

            df = source_dfs[0]
            return df.groupBy("user_id").agg(
                sum("amount").alias("total_amount"),
                count("*").alias("transaction_count")
            )

        platform.gold.create_aggregate(
            source_tables=[platform.bronze.table_path("transactions_raw")],
            target_table="user_transaction_summary",
            aggregation_func=aggregate_transactions,
            mode="overwrite"
        )

        print("Gold layer aggregation complete!")

        # Optimize tables
        print("Optimizing tables...")
        platform.optimize_all_tables()

        print("Pipeline complete!")


if __name__ == "__main__":
    main()
