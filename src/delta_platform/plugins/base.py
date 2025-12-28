"""Base plugin interface."""

from abc import ABC, abstractmethod
from typing import Any


class Plugin(ABC):
    """
    Base class for Delta Platform plugins.

    Plugins allow users to extend the platform with custom functionality.
    """

    @abstractmethod
    def initialize(self, platform: Any) -> None:
        """Initialize the plugin.

        Args:
            platform: Platform instance

        Example:
            >>> class MyPlugin(Plugin):
            ...     def initialize(self, platform):
            ...         print("Plugin initialized!")
        """
        pass

    @abstractmethod
    def get_name(self) -> str:
        """Get plugin name.

        Returns:
            Plugin name
        """
        pass

    def on_table_created(self, table: Any) -> None:
        """Hook called when a table is created.

        Args:
            table: DeltaTable instance
        """
        pass

    def on_table_write(self, table: Any, df: Any) -> None:
        """Hook called before writing to a table.

        Args:
            table: DeltaTable instance
            df: DataFrame being written
        """
        pass

    def on_table_read(self, table: Any, df: Any) -> Any:
        """Hook called after reading from a table.

        Args:
            table: DeltaTable instance
            df: DataFrame that was read

        Returns:
            Modified DataFrame (or original if no modification)
        """
        return df
