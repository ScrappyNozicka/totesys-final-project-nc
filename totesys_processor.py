class ToteSysProcessor:
    """Handles extraction of table names, row IDs, and last_updated timestamps from ToteSys data."""

    @staticmethod
    def get_table_names(totesys_data):
        return list(totesys_data.keys())

    @staticmethod
    def get_row_id(row, table_name):
        return row[f"{table_name}_id"]

    @staticmethod
    def get_last_updated(row):
        return row["last_updated"]
