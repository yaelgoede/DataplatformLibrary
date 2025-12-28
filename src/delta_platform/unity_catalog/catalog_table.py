"""Unity Catalog table implementation."""

from typing import Optional, List, Dict, Any
from pyspark.sql import SparkSession, DataFrame

from delta_platform.core.table import DeltaTable
from delta_platform.core.table_config import TableConfig, MedallionLayer


class UnityCatalogTable(DeltaTable):
    """
    Delta table with Unity Catalog integration.

    Provides additional Unity Catalog-specific operations like
    permissions, lineage, and metadata management.
    """

    def __init__(
        self,
        spark: SparkSession,
        catalog: str,
        schema: str,
        table: str,
        layer: Optional[MedallionLayer] = None
    ):
        """Initialize Unity Catalog table.

        Args:
            spark: Active SparkSession
            catalog: Catalog name
            schema: Schema name
            table: Table name
            layer: Medallion layer (inferred if not provided)

        Example:
            >>> table = UnityCatalogTable(spark, "main", "bronze", "users")
        """
        self.catalog = catalog
        self.schema_name = schema
        self.table_name = table
        self.fqn = f"{catalog}.{schema}.{table}"

        # Infer layer from schema name if not provided
        if layer is None:
            schema_lower = schema.lower()
            if "bronze" in schema_lower:
                layer = MedallionLayer.BRONZE
            elif "silver" in schema_lower:
                layer = MedallionLayer.SILVER
            elif "gold" in schema_lower:
                layer = MedallionLayer.GOLD
            else:
                layer = MedallionLayer.BRONZE

        # Get table location
        try:
            table_info = spark.sql(f"DESCRIBE DETAIL {self.fqn}").collect()[0]
            path = table_info.location
        except:
            path = f"/unity_catalog/{catalog}/{schema}/{table}"

        config = TableConfig(
            name=table,
            path=path,
            layer=layer,
            description=f"Unity Catalog: {self.fqn}"
        )

        super().__init__(spark, config)

    def grant_permissions(
        self,
        principal: str,
        privileges: List[str]
    ) -> None:
        """Grant permissions on table.

        Args:
            principal: User or group name
            privileges: List of privileges (SELECT, MODIFY, etc.)

        Example:
            >>> table.grant_permissions("data_engineers", ["SELECT", "MODIFY"])
        """
        for privilege in privileges:
            grant_sql = f"GRANT {privilege} ON TABLE {self.fqn} TO `{principal}`"
            self.spark.sql(grant_sql)

    def revoke_permissions(
        self,
        principal: str,
        privileges: List[str]
    ) -> None:
        """Revoke permissions on table.

        Args:
            principal: User or group name
            privileges: List of privileges to revoke
        """
        for privilege in privileges:
            revoke_sql = f"REVOKE {privilege} ON TABLE {self.fqn} FROM `{principal}`"
            self.spark.sql(revoke_sql)

    def get_lineage(self) -> Dict[str, Any]:
        """Get data lineage information from Unity Catalog.

        Returns:
            Dictionary with lineage information

        Note:
            Requires Unity Catalog lineage tracking to be enabled
        """
        # This would use Unity Catalog's lineage API
        # Placeholder implementation
        return {
            "table": self.fqn,
            "upstream": [],
            "downstream": [],
            "note": "Lineage tracking requires Unity Catalog API access"
        }

    def add_tags(self, tags: Dict[str, str]) -> None:
        """Add tags to the table in Unity Catalog.

        Args:
            tags: Dictionary of tag key-value pairs

        Example:
            >>> table.add_tags({"pii": "true", "owner": "data-team"})
        """
        for key, value in tags.items():
            alter_sql = f"ALTER TABLE {self.fqn} SET TAGS ('{key}' = '{value}')"
            self.spark.sql(alter_sql)

    def set_owner(self, owner: str) -> None:
        """Set table owner in Unity Catalog.

        Args:
            owner: Owner principal (user or group)
        """
        alter_sql = f"ALTER TABLE {self.fqn} OWNER TO `{owner}`"
        self.spark.sql(alter_sql)

    def get_catalog_info(self) -> Dict[str, Any]:
        """Get Unity Catalog metadata.

        Returns:
            Dictionary with catalog metadata
        """
        info_df = self.spark.sql(f"DESCRIBE EXTENDED {self.fqn}")
        info_dict = {row["col_name"]: row["data_type"] for row in info_df.collect()}

        return {
            "catalog": self.catalog,
            "schema": self.schema_name,
            "table": self.table_name,
            "fqn": self.fqn,
            "owner": info_dict.get("Owner"),
            "created_by": info_dict.get("Created By"),
            "location": info_dict.get("Location"),
            "provider": info_dict.get("Provider")
        }
