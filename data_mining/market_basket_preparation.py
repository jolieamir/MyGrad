"""
Market Basket Data Preparation Module
Utility functions for preparing transaction data from Cleaned_data
for use with Apriori / FP-Growth algorithms.

Three approaches demonstrated:
  1. SQL (STRING_AGG)
  2. Pandas (groupby + agg)
  3. PySpark (collect_list)
"""

import pandas as pd
import pyodbc
from config import Config
from typing import List


class MarketBasketDataPreparator:
    """Fetches and prepares transactional data for mining algorithms."""

    def __init__(self):
        self.conn = None

    def _connect(self):
        if self.conn is None:
            conn_str = Config.get_sql_server_connection_string()
            self.conn = pyodbc.connect(conn_str)
        return self.conn

    # ------------------------------------------------------------------
    # SQL approach: STRING_AGG (SQL Server 2017+)
    # ------------------------------------------------------------------
    def get_transactions_sql(self) -> pd.DataFrame:
        """Group by InvoiceNo using STRING_AGG. Returns DataFrame[InvoiceNo, Items]."""
        conn = self._connect()
        query = """
            SELECT
                InvoiceNo,
                STRING_AGG(DISTINCT CAST(Description AS VARCHAR(MAX)), '||') AS Items
            FROM Cleaned_data
            WHERE Description IS NOT NULL
              AND LTRIM(RTRIM(Description)) <> ''
            GROUP BY InvoiceNo
            ORDER BY InvoiceNo
        """
        df = pd.read_sql(query, conn)
        df['Items'] = df['Items'].apply(
            lambda x: list(set(x.split('||'))) if pd.notna(x) else []
        )
        return df

    # ------------------------------------------------------------------
    # Pandas approach
    # ------------------------------------------------------------------
    def get_transactions_pandas(self, df_raw: pd.DataFrame = None) -> pd.DataFrame:
        """Group by InvoiceNo using pandas. Returns DataFrame[InvoiceNo, Items]."""
        if df_raw is None:
            conn = self._connect()
            query = (
                "SELECT InvoiceNo, Description FROM Cleaned_data "
                "WHERE Description IS NOT NULL AND LTRIM(RTRIM(Description)) <> ''"
            )
            df_raw = pd.read_sql(query, conn)

        df_clean = df_raw.drop_duplicates(subset=['InvoiceNo', 'Description'])
        df_txn = df_clean.groupby('InvoiceNo')['Description'].agg(list).reset_index()
        df_txn.columns = ['InvoiceNo', 'Items']
        return df_txn

    # ------------------------------------------------------------------
    # PySpark approach
    # ------------------------------------------------------------------
    @staticmethod
    def get_transactions_pyspark(spark) -> 'pyspark.sql.DataFrame':
        """Group by InvoiceNo using PySpark. Requires a SparkSession."""
        cfg = Config.SQL_SERVER_CONFIG
        jdbc_url = f"jdbc:sqlserver://{cfg['server']};databaseName={cfg['database']}"
        properties = {
            "user": cfg.get('username', 'sa'),
            "password": cfg.get('password', ''),
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        }

        from pyspark.sql import functions as F

        df = spark.read.jdbc(url=jdbc_url, table="Cleaned_data", properties=properties)
        df_filtered = df.filter(
            F.col("Description").isNotNull() & (F.trim(F.col("Description")) != "")
        )
        return (
            df_filtered.select("InvoiceNo", "Description")
            .distinct()
            .groupBy("InvoiceNo")
            .agg(F.collect_list("Description").alias("Items"))
            .orderBy("InvoiceNo")
        )

    # ------------------------------------------------------------------
    # Conversion helpers
    # ------------------------------------------------------------------
    @staticmethod
    def to_transaction_list(df_transactions: pd.DataFrame) -> List[List[str]]:
        """Convert DataFrame[InvoiceNo, Items] to list-of-lists for mining."""
        return df_transactions['Items'].tolist()

    @staticmethod
    def get_statistics(df_transactions: pd.DataFrame) -> dict:
        """Compute summary statistics from a transactions DataFrame."""
        if df_transactions.empty:
            return {}
        lengths = df_transactions['Items'].apply(len)
        all_items = set(item for items in df_transactions['Items'] for item in items)
        return {
            'total_transactions': len(df_transactions),
            'unique_items': len(all_items),
            'avg_items': round(lengths.mean(), 2),
            'min_items': int(lengths.min()),
            'max_items': int(lengths.max()),
            'median_items': round(lengths.median(), 2),
        }

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def __del__(self):
        self.close()
