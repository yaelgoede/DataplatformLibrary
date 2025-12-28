"""Main CLI entry point."""

def main():
    """Main CLI entry point."""
    import sys

    print("Delta Platform CLI")
    print("==================")
    print()
    print("Available commands:")
    print("  table create      - Create a new Delta table")
    print("  table optimize    - Optimize a table")
    print("  table vacuum      - Vacuum old files")
    print("  table stats       - Show table statistics")
    print("  validate          - Run validation checks")
    print("  profile           - Generate data profile")
    print("  optimize-advisor  - Get optimization recommendations")
    print()
    print("For full CLI functionality, install with:")
    print("  pip install delta-platform[cli]")
    print()
    print("Then use:")
    print("  delta-platform --help")


if __name__ == "__main__":
    main()
