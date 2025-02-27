from decimal import Decimal

class ToteSysProcessor:
    """
    Handles extraction of table names, row IDs, and
    last_updated timestamps from ToteSys data.
    """

    @staticmethod
    def get_table_names(totesys_data: dict[list[dict]]) -> list[str]:
        """
        Function for getting table name from ToteSys Data

        Returns:
            list: List of Table Names
        """
        return list(totesys_data.keys())

    @staticmethod
    def get_row_id(row, table_name: str) -> str:
        """
        Function for getting row_id from table data

        Returns:
            str: row_id
        """
        return row[f"{table_name}_id"]

    @staticmethod
    def get_last_updated(row: dict) -> str:
        """
        Function for getting last_updated table data

        Returns:
            str: last_updated
        """
        return row["last_updated"]
    
    @staticmethod
    def decimal_to_str(value):
        """Convert Decimal to string to avoid precision loss."""
        if isinstance(value, Decimal):
            return str(value)
        raise TypeError(f"Type {type(value)} not serializable")
