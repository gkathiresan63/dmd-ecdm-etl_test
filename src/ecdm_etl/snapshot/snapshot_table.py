"""Snapshot Table definition for EnterpriseCustomerDataManagement project."""


class SnapshotTable:
    """Snapshot Table definition."""

    def __init__(self, source_table_name: str, snapshot_table_name: str, snapshot_merge_columns,) -> None:
        """Create a new snapshot table."""
        self.source_table_name = source_table_name
        self.snapshot_table_name = snapshot_table_name
        if isinstance(snapshot_merge_columns, str):
            self.snapshot_merge_columns = snapshot_merge_columns.split(",")
        else:
            self.snapshot_merge_columns = snapshot_merge_columns
