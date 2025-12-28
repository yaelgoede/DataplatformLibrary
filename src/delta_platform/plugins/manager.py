"""Plugin manager."""

from typing import List, Dict, Any
from delta_platform.plugins.base import Plugin


class PluginManager:
    """
    Manages plugins for the Delta Platform.

    Allows registration and lifecycle management of plugins.
    """

    def __init__(self):
        """Initialize plugin manager."""
        self.plugins: Dict[str, Plugin] = {}

    def register(self, plugin: Plugin) -> None:
        """Register a plugin.

        Args:
            plugin: Plugin instance

        Example:
            >>> manager = PluginManager()
            >>> manager.register(MyCustomPlugin())
        """
        name = plugin.get_name()
        self.plugins[name] = plugin

    def initialize_all(self, platform: Any) -> None:
        """Initialize all registered plugins.

        Args:
            platform: Platform instance
        """
        for plugin in self.plugins.values():
            plugin.initialize(platform)

    def get_plugin(self, name: str) -> Plugin:
        """Get a plugin by name.

        Args:
            name: Plugin name

        Returns:
            Plugin instance

        Raises:
            KeyError: If plugin not found
        """
        return self.plugins[name]

    def list_plugins(self) -> List[str]:
        """List all registered plugin names.

        Returns:
            List of plugin names
        """
        return list(self.plugins.keys())

    def trigger_table_created(self, table: Any) -> None:
        """Trigger on_table_created hook for all plugins.

        Args:
            table: DeltaTable instance
        """
        for plugin in self.plugins.values():
            plugin.on_table_created(table)

    def trigger_table_write(self, table: Any, df: Any) -> None:
        """Trigger on_table_write hook for all plugins.

        Args:
            table: DeltaTable instance
            df: DataFrame being written
        """
        for plugin in self.plugins.values():
            plugin.on_table_write(table, df)

    def trigger_table_read(self, table: Any, df: Any) -> Any:
        """Trigger on_table_read hook for all plugins.

        Args:
            table: DeltaTable instance
            df: DataFrame that was read

        Returns:
            Potentially modified DataFrame
        """
        result_df = df
        for plugin in self.plugins.values():
            result_df = plugin.on_table_read(table, result_df)
        return result_df
