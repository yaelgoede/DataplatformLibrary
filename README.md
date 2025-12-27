# Delta Platform Library

A comprehensive Python library for managing Delta Lake data platforms with medallion architecture on Databricks Spark.

## Features

- **Medallion Architecture**: Built-in support for Bronze, Silver, and Gold layers
- **Multiple Format Ingestion**: Support for JSON, XML, Parquet, and CSV formats
- **Data Quality**: Built-in deduplication, cleaning, and validation
- **Slowly Changing Dimensions**: SCD Type 2 implementation
- **Streaming Support**: Real-time data ingestion with Spark Structured Streaming
- **Advanced Transformations**: Merge updates, time-series aggregations, star schema creation
- **Optimization**: Automatic table optimization and vacuuming
- **Configuration Management**: YAML-based configuration support

## Installation

Install using uv (recommended):

```bash
uv pip install delta-platform
```

Or using pip:

```bash
pip install delta-platform
```

## Quick Start

```python
from delta_platform import DeltaPlatform, PlatformConfig

# Create configuration
config = PlatformConfig.create_default("/mnt/delta")

# Initialize platform
with DeltaPlatform(config) as platform:
    # Ingest data into Bronze layer
    platform.bronze.ingest_json(
        source_path="/data/raw/users.json",
        table_name="users_raw"
    )

    # Transform to Silver layer
    def clean_data(df):
        return df.dropna().dropDuplicates()

    platform.silver.transform_from_bronze(
        bronze_table=platform.bronze.table_path("users_raw"),
        silver_table="users_clean",
        transformation_func=clean_data
    )

    # Create Gold layer aggregations
    def aggregate(dfs):
        return dfs[0].groupBy("category").count()

    platform.gold.create_aggregate(
        source_tables=[platform.silver.table_path("users_clean")],
        target_table="user_summary",
        aggregation_func=aggregate
    )
```

## Architecture

### Medallion Architecture Layers

The library implements the medallion architecture pattern with three distinct layers:

#### Bronze Layer (Raw Data)
- Ingests raw data from various sources
- Preserves original data format
- Adds ingestion metadata (timestamp, source file)
- Supports batch and streaming ingestion

#### Silver Layer (Cleaned Data)
- Applies data quality rules
- Performs deduplication and cleaning
- Implements business logic transformations
- Supports SCD Type 2 and merge operations

#### Gold Layer (Business-Level Aggregations)
- Creates dimension and fact tables
- Builds aggregated summaries
- Supports star schema modeling
- Enables time-series analysis

## Configuration

### Using YAML Configuration

Create a `config.yaml` file:

```yaml
bronze:
  path: /mnt/delta/bronze
  checkpoint_path: /mnt/delta/bronze/_checkpoints

silver:
  path: /mnt/delta/silver
  checkpoint_path: /mnt/delta/silver/_checkpoints
  partition_columns:
    - date

gold:
  path: /mnt/delta/gold
  checkpoint_path: /mnt/delta/gold/_checkpoints

spark_config:
  spark.databricks.delta.optimizeWrite.enabled: "true"
  spark.databricks.delta.autoCompact.enabled: "true"
```

Load the configuration:

```python
config = PlatformConfig.from_yaml("config.yaml")
platform = DeltaPlatform(config)
```

## Usage Examples

### Data Ingestion

#### JSON Ingestion
```python
platform.bronze.ingest_json(
    source_path="/data/raw/*.json",
    table_name="events",
    multiline=True,
    mode="append"
)
```

#### XML Ingestion
```python
platform.bronze.ingest_xml(
    source_path="/data/raw/*.xml",
    table_name="products",
    row_tag="product",
    mode="overwrite"
)
```

#### Parquet Ingestion
```python
platform.bronze.ingest_parquet(
    source_path="/data/raw/*.parquet",
    table_name="transactions",
    partition_by=["year", "month"]
)
```

#### CSV Ingestion
```python
platform.bronze.ingest_csv(
    source_path="/data/raw/*.csv",
    table_name="customers",
    header=True,
    delimiter=","
)
```

### Streaming Ingestion

```python
query = platform.bronze.ingest_streaming(
    source_path="/data/stream",
    table_name="realtime_events",
    format="json",
    trigger_interval="10 seconds"
)
```

### Data Transformation

#### Basic Transformation
```python
def transform(df):
    from pyspark.sql.functions import trim, upper
    return df.select(
        "id",
        trim(upper("name")).alias("name"),
        "email"
    ).dropna()

platform.silver.transform_from_bronze(
    bronze_table=platform.bronze.table_path("users_raw"),
    silver_table="users_clean",
    transformation_func=transform,
    deduplicate_columns=["id"]
)
```

#### Merge Updates
```python
updates_df = spark.read.parquet("/data/updates")

platform.silver.merge_updates(
    source_df=updates_df,
    target_table="products",
    merge_keys=["product_id"],
    update_columns=["price", "stock"]
)
```

#### Slowly Changing Dimension Type 2
```python
platform.silver.apply_scd_type2(
    source_df=customer_updates,
    target_table="customers_history",
    natural_keys=["customer_id"],
    start_date_col="valid_from",
    end_date_col="valid_to"
)
```

### Gold Layer Analytics

#### Create Summary Table
```python
platform.gold.create_summary(
    source_table=platform.silver.table_path("sales"),
    summary_table="sales_by_region",
    group_by=["region", "product_category"],
    aggregations={
        "amount": "sum",
        "quantity": "sum",
        "order_id": "count"
    }
)
```

#### Create Dimension Table
```python
platform.gold.create_dimension(
    source_table=platform.silver.table_path("customers"),
    dimension_table="dim_customer",
    dimension_columns=["customer_id", "name", "region"],
    surrogate_key="customer_key"
)
```

#### Create Fact Table
```python
def join_sources(sources):
    return sources["orders"] \
        .join(sources["customers"], "customer_id") \
        .join(sources["products"], "product_id")

platform.gold.create_fact_table(
    source_tables={
        "orders": platform.silver.table_path("orders"),
        "customers": platform.silver.table_path("customers"),
        "products": platform.silver.table_path("products")
    },
    fact_table="fact_sales",
    join_func=join_sources,
    measures=["quantity", "amount"],
    dimensions=["customer_id", "product_id", "date"]
)
```

#### Time-Series Aggregation
```python
platform.gold.create_time_series(
    source_table=platform.silver.table_path("events"),
    time_series_table="daily_metrics",
    timestamp_column="event_timestamp",
    group_by=["region"],
    metrics={"revenue": "sum", "users": "count"},
    time_grain="day"
)
```

### Table Maintenance

#### Optimize Tables
```python
# Optimize single table
platform.bronze.optimize_table("users", z_order_by=["user_id"])

# Optimize all tables
platform.optimize_all_tables()
```

#### Vacuum Tables
```python
# Vacuum single table (remove old files)
platform.silver.vacuum_table("customers", retention_hours=168)

# Vacuum all tables
platform.vacuum_all_tables(retention_hours=168)
```

## Advanced Features

### Custom Spark Session

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CustomApp") \
    .config("spark.some.config", "value") \
    .getOrCreate()

platform = DeltaPlatform(config, spark=spark)
```

### Data Quality Checks

```python
# Clean data with built-in utilities
cleaned_df = platform.silver.clean_data(
    df,
    drop_duplicates=True,
    drop_nulls=["id", "email"],
    trim_strings=True
)
```

### Table Information

```python
info = platform.get_table_info("silver", "customers")
print(f"Table version: {info['version']}")
print(f"Schema: {info['schema']}")
```

## Best Practices

1. **Use Configuration Files**: Store your platform configuration in YAML for easy environment management
2. **Partition Strategy**: Partition large tables by date or other high-cardinality columns
3. **Regular Optimization**: Run `optimize_all_tables()` regularly to maintain query performance
4. **Vacuum Old Files**: Use `vacuum_all_tables()` to remove old data files and reduce storage costs
5. **Schema Evolution**: Enable `merge_schema=True` for handling schema changes
6. **Checkpointing**: Always specify checkpoint paths for streaming workloads
7. **Deduplication**: Use deduplication in Silver layer to ensure data quality
8. **Incremental Processing**: Use merge operations for incremental updates

## Contributing

Contributions are welcome! Please feel free to submit issues and pull requests.

## License

This project is licensed under the MIT License.

## Support

For questions and support, please open an issue on GitHub.

## Examples

See the `examples/` directory for complete working examples:
- `basic_usage.py`: Basic medallion architecture pipeline
- `advanced_usage.py`: Advanced features including SCD, streaming, and star schema
- `config.yaml`: Example configuration file
