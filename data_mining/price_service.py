"""
Price Service Module
Fetches product prices from SQL Server Cleaned_data table.
"""

import pyodbc
from config import Config


class PriceService:
    def __init__(self):
        self.conn = None
        self.connected = False

    def _connect(self):
        """Establish connection using the centralized config."""
        try:
            conn_str = Config.get_sql_server_connection_string()
            self.conn = pyodbc.connect(conn_str)
            self.connected = True
            return True
        except Exception as e:
            print(f"PriceService: SQL Server connection failed: {e}")
            self.connected = False
            return False

    def _ensure_connected(self):
        if not self.connected:
            return self._connect()
        return True

    def is_connected(self):
        return self.connected

    def get_product_price(self, product_name):
        """Get the average UnitPrice for a product from Cleaned_data."""
        if not self._ensure_connected():
            return None
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT AVG(CAST(UnitPrice AS FLOAT)) FROM Cleaned_data "
                "WHERE Description = ? AND UnitPrice > 0",
                (product_name,),
            )
            row = cursor.fetchone()
            return round(float(row[0]), 2) if row and row[0] else None
        except Exception as e:
            print(f"PriceService: Error fetching price for '{product_name}': {e}")
            return None

    def get_all_product_prices(self):
        """Get average prices for all products in Cleaned_data.

        Returns:
            dict mapping Description -> avg_price
        """
        if not self._ensure_connected():
            return {}
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT Description, AVG(CAST(UnitPrice AS FLOAT)) AS avg_price "
                "FROM Cleaned_data WHERE UnitPrice > 0 "
                "GROUP BY Description"
            )
            return {row[0]: round(float(row[1]), 2) for row in cursor.fetchall()}
        except Exception as e:
            print(f"PriceService: Error fetching all prices: {e}")
            return {}

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
            self.connected = False

    def __del__(self):
        self.close()
