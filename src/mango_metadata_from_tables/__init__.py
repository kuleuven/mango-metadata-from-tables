from rich.console import Console

DATAOBJECT = "dataobject"
EXCLUDE_NONSCHEMA_MD = "exclude_non_schema_metadata"
EXCLUDE_INVALID_SCHEMA_MD = "exclude_invalid_schema_metadata"
console = Console()


def main() -> None:
    print("Hello from mango-metadata-from-tables!")
