# Delta Platform Library

A comprehensive Python library for managing Delta Lake data platforms with medallion architecture on Databricks Spark, built with clean design patterns for maintainability and extensibility.

## 🎯 Features

- **Table-Centric Design**: Manage individual Delta tables with full control over operations
- **Design Patterns**: Built using Builder, Strategy, and Repository patterns
- **Medallion Architecture**: Bronze, Silver, and Gold layers
- **Multiple Formats**: JSON, XML, Parquet, CSV ingestion
- **Pluggable Transformations**: Strategy pattern for flexible data transformations
- **Data Quality**: Cleaning, deduplication, and validation strategies
- **Merge Operations**: Upserts, SCD Type 2, incremental updates
- **Optimization**: Z-ordering, vacuuming, and table maintenance
- **Method Chaining**: Fluent interface for readable code

## 📦 Installation

Install using uv (recommended):

```bash
uv pip install delta-platform
```

Or using pip:

```bash
pip install delta-platform
```

## 🚀 Quick Start

### Managing a Single Table

```python
from pyspark.sql import SparkSession
from delta_platform import TableBuilder, MedallionLayer, JsonDataSource, CleaningStrategy

# Create Spark session
spark = SparkSession.builder.getOrCreate()

# Build a table using the Builder pattern
users_table = (TableBuilder(spark)
    .name("users")
    .path("/data/bronze/users")
    .layer(MedallionLayer.BRONZE)
    .partition_by(["country"])
    .z_order_by(["user_id"])
    .build())

# Ingest data using Repository pattern
source = JsonDataSource(spark, "/raw/users/*.json")
users_df = source.read(multiline=True)

# Append data with deduplication
users_table.append(users_df, deduplicate=True, deduplicate_columns=["user_id"])

# Transform using Strategy pattern
cleaning_strategy = CleaningStrategy(
    drop_duplicates=True,
    drop_null_columns=["user_id", "email"],
    trim_strings=True
)

clean_table = (TableBuilder(spark)
    .name("users_clean")
    .path("/data/silver/users")
    .layer(MedallionLayer.SILVER)
    .build())

users_table.transform(cleaning_strategy, target_table=clean_table)

# Optimize and maintain
users_table.optimize().vacuum()
```

## 🏗️ Architecture & Design Patterns

### Builder Pattern (Table Configuration)

The Builder pattern provides a fluent interface for creating complex table configurations:

```python
table = (TableBuilder(spark)
    .name("orders")
    .path("/data/bronze/orders")
    .layer(MedallionLayer.BRONZE)
    .partition_by(["order_date", "region"])
    .z_order_by(["customer_id"])
    .merge_schema(True)
    .optimize_write(True)
    .description("Order transactions from payment system")
    .tag("owner", "data-team")
    .tag("pii", "false")
    .build())
```

### Strategy Pattern (Transformations)

Different transformation strategies can be swapped without changing the table code:

```python
from delta_platform import (
    CleaningStrategy,
    DeduplicationStrategy,
    FilterStrategy,
    AggregationStrategy,
    CustomStrategy
)

# Cleaning strategy
cleaning = CleaningStrategy(
    drop_duplicates=True,
    drop_null_columns=["id"],
    trim_strings=True
)

# Deduplication strategy (keep latest)
dedupe = DeduplicationStrategy(
    dedupe_columns=["user_id"],
    order_by="timestamp",
    keep="last"
)

# Filter strategy
filter_active = FilterStrategy("status = 'active'")

# Aggregation strategy
daily_summary = AggregationStrategy(
    group_by=["date", "region"],
    aggregations={"revenue": "sum", "orders": "count"}
)

# Custom strategy
def my_transform(df):
    return df.filter(df.amount > 0).withColumn("category", ...)

custom = CustomStrategy(my_transform)

# Apply any strategy
table.transform(cleaning, target_table=clean_table)
```

### Repository Pattern (Data Sources)

Abstract data source access for different formats:

```python
from delta_platform import (
    JsonDataSource,
    XmlDataSource,
    ParquetDataSource,
    CsvDataSource
)

# JSON
json_source = JsonDataSource(spark, "/data/events/*.json")
df = json_source.read(multiline=True)

# XML
xml_source = XmlDataSource(spark, "/data/products.xml")
df = xml_source.read(row_tag="product")

# Parquet
parquet_source = ParquetDataSource(spark, "/data/transactions/*.parquet")
df = parquet_source.read()

# CSV
csv_source = CsvDataSource(spark, "/data/customers.csv")
df = csv_source.read(header=True, delimiter=",")

# Streaming
stream_df = json_source.read_stream()
```

## 📊 Table Operations

### Append Data

```python
# Simple append
table.append(df)

# Append with metadata
table.append(df, add_metadata=True)

# Append with deduplication
table.append(df, deduplicate=True, deduplicate_columns=["id"])
```

### Merge (Upsert)

```python
# Merge updates
table.merge(
    source_df=updates_df,
    merge_keys=["product_id"],
    update_columns=["price", "stock"]
)

# Insert-only merge
table.merge(
    source_df=new_records_df,
    merge_keys=["id"],
    insert_only=True
)

# Merge with delete
table.merge(
    source_df=updates_df,
    merge_keys=["id"],
    delete_condition="source.is_deleted = true"
)
```

### Transform Data

```python
# Using predefined strategies
table.transform(CleaningStrategy(...), target_table=target)

# Using custom transformation
def enrich_data(df):
    return df.withColumn("enriched_field", ...)

table.transform(CustomStrategy(enrich_data), target_table=target)

# Transform without target (just get DataFrame)
transformed_df = table.transform(strategy)
```

### Table Maintenance

```python
# Optimize with Z-ordering
table.optimize(z_order_by=["user_id", "date"])

# Vacuum old files
table.vacuum(retention_hours=168)  # 7 days

# Get statistics
stats = table.statistics()
print(stats)

# View history
history = table.history(limit=10)
history.show()

# Chain operations
table.append(df).optimize().vacuum()
```

## 🏅 Medallion Architecture Example

Complete Bronze → Silver → Gold pipeline:

```python
from delta_platform import TableBuilder, MedallionLayer, CleaningStrategy, AggregationStrategy

# BRONZE: Ingest raw data
bronze_table = (TableBuilder(spark)
    .name("events_raw")
    .path("/data/bronze/events")
    .layer(MedallionLayer.BRONZE)
    .build())

source = JsonDataSource(spark, "/raw/events/*.json")
bronze_table.append(source.read())

# SILVER: Clean and validate
silver_table = (TableBuilder(spark)
    .name("events_clean")
    .path("/data/silver/events")
    .layer(MedallionLayer.SILVER)
    .partition_by(["date"])
    .build())

cleaning = CleaningStrategy(
    drop_null_columns=["event_id", "user_id"],
    trim_strings=True
)

bronze_table.transform(cleaning, target_table=silver_table)

# GOLD: Aggregate for analytics
gold_table = (TableBuilder(spark)
    .name("daily_metrics")
    .path("/data/gold/daily_metrics")
    .layer(MedallionLayer.GOLD)
    .build())

aggregation = AggregationStrategy(
    group_by=["date", "event_type"],
    aggregations={"event_id": "count", "revenue": "sum"}
)

silver_table.transform(aggregation, target_table=gold_table)
```

## 🔄 Common Patterns

### Incremental Processing

```python
# Read new data
new_data = source.read()

# Merge into existing table
table.merge(
    source_df=new_data,
    merge_keys=["id"],
    update_columns=["updated_at", "status"]
)
```

### Slowly Changing Dimension (SCD Type 2)

```python
from pyspark.sql.functions import current_timestamp, lit

def scd_transform(df):
    return df \
        .withColumn("effective_start_date", current_timestamp()) \
        .withColumn("effective_end_date", lit(None).cast("timestamp")) \
        .withColumn("is_current", lit(True))

# Apply SCD logic
scd_strategy = CustomStrategy(scd_transform)
source_table.transform(scd_strategy, target_table=scd_table)
```

### Data Quality Checks

```python
# Clean and validate in one pass
quality_strategy = CleaningStrategy(
    drop_duplicates=True,
    drop_null_columns=["required_field1", "required_field2"],
    trim_strings=True,
    fill_null_values={"optional_field": "default"}
)

table.transform(quality_strategy, target_table=clean_table)
```

### Deduplication

```python
# Keep latest record per key
dedupe_strategy = DeduplicationStrategy(
    dedupe_columns=["customer_id"],
    order_by="updated_at",
    keep="last"
)

table.transform(dedupe_strategy, target_table=dedupe_table)
```

## 🎓 Best Practices

1. **Use the Builder Pattern** for table configuration - it's more readable and maintainable
2. **Leverage Strategy Pattern** for transformations - makes your code testable and reusable
3. **Partition Large Tables** by date or high-cardinality columns
4. **Enable Z-Ordering** on frequently filtered columns
5. **Regular Optimization** - run `optimize()` after large writes
6. **Vacuum Periodically** - clean up old files with `vacuum()`
7. **Chain Operations** - use method chaining for concise code
8. **Add Metadata** - always track ingestion timestamps and sources
9. **Deduplicate at Bronze** - catch duplicates early in the pipeline
10. **Use Merge for Updates** - more efficient than delete+insert

## 📚 Examples

The `examples/` directory contains comprehensive examples:

- `table_management.py` - Single table operations and design patterns
- `medallion_pipeline.py` - Complete Bronze→Silver→Gold pipeline
- `basic_usage.py` - Legacy platform-level API (still supported)
- `advanced_usage.py` - Advanced features and patterns

## 🔧 Configuration

### Programmatic Configuration

```python
from delta_platform import TableConfig, MedallionLayer

config = TableConfig(
    name="my_table",
    path="/data/bronze/my_table",
    layer=MedallionLayer.BRONZE,
    partition_columns=["date", "region"],
    z_order_columns=["user_id"],
    optimize_write=True,
    auto_compact=True
)

table = TableBuilder.from_config(spark, config)
```

### YAML Configuration

```yaml
# config.yaml
tables:
  users:
    name: users
    path: /data/bronze/users
    layer: bronze
    partition_columns:
      - country
    z_order_columns:
      - user_id
```

## 🆚 Design Pattern Benefits

### Before (Procedural)
```python
df = spark.read.json("/data/raw/*.json")
df = df.dropDuplicates()
df = df.dropna(subset=["id"])
df.write.format("delta").mode("append").save("/data/bronze/table")
```

### After (Design Patterns)
```python
table = TableBuilder(spark).name("table").path("/data/bronze/table").layer(BRONZE).build()
source = JsonDataSource(spark, "/data/raw/*.json")
strategy = CleaningStrategy(drop_duplicates=True, drop_null_columns=["id"])
table.append(source.read()).transform(strategy).optimize()
```

**Benefits:**
- ✅ More readable and maintainable
- ✅ Reusable strategies
- ✅ Testable components
- ✅ Extensible architecture
- ✅ Method chaining
- ✅ Type safety

## 📖 API Reference

### Core Classes

- `DeltaTable` - Manages a single Delta table with all operations
- `TableBuilder` - Builder pattern for table configuration
- `TableConfig` - Table configuration dataclass
- `MedallionLayer` - Enum for Bronze/Silver/Gold layers

### Strategies

- `CleaningStrategy` - Data cleaning operations
- `DeduplicationStrategy` - Remove duplicates
- `FilterStrategy` - Filter data by condition
- `AggregationStrategy` - Group and aggregate
- `CustomStrategy` - User-defined transformations

### Repositories

- `JsonDataSource` - Read JSON data
- `XmlDataSource` - Read XML data
- `ParquetDataSource` - Read Parquet data
- `CsvDataSource` - Read CSV data
- `DeltaDataSource` - Read Delta Lake data

## 🔄 Migration from v0.1

The library maintains backward compatibility with the v0.1 platform-level API:

```python
# v0.1 (still works)
from delta_platform import DeltaPlatform, PlatformConfig
platform = DeltaPlatform(config)
platform.bronze.ingest_json(...)

# v0.2 (recommended)
from delta_platform import TableBuilder, JsonDataSource
table = TableBuilder(spark).name(...).build()
source = JsonDataSource(spark, path)
table.append(source.read())
```

## 🤝 Contributing

Contributions are welcome! The library follows SOLID principles and design patterns.

## 📄 License

MIT License - see LICENSE file for details.

## 📞 Support

- GitHub Issues: Report bugs and request features
- Documentation: See `examples/` for working code
- Design Patterns: Builder, Strategy, Repository patterns used throughout

## 🎯 Roadmap

- [ ] Factory pattern for table creation from metadata
- [ ] Observer pattern for data quality monitoring
- [ ] Command pattern for complex pipelines
- [ ] Additional transformation strategies
- [ ] Unity Catalog integration
- [ ] Delta Sharing support
