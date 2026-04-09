"""
Data Pipeline Module
Handles CSV upload -> SQL Server insertion -> cleaning -> transaction preparation.

Pipeline per upload:
  1. TRUNCATE Row_data and Cleaned_data
  2. Insert raw CSV into Row_data
  3. Copy Row_data into Cleaned_data
  4. Apply cleaning rules on Cleaned_data
  5. Extract transactions grouped by InvoiceNo (deduplicated)
"""

import pandas as pd
import pyodbc
from config import Config


class DataPipeline:
    def __init__(self):
        self.conn = None

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------
    def _get_connection(self):
        """Return an open pyodbc connection (lazy, reusable)."""
        if self.conn is None:
            conn_str = Config.get_sql_server_connection_string()
            self.conn = pyodbc.connect(conn_str, autocommit=False)
        return self.conn

    def test_connection(self):
        """Return True if SQL Server is reachable."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            return True
        except Exception as e:
            print(f"SQL Server connection test failed: {e}")
            return False

    # ------------------------------------------------------------------
    # Full pipeline: CSV -> Row_data -> Cleaned_data -> transactions
    # ------------------------------------------------------------------
    def run_pipeline(self, csv_path):
        """Execute the complete data pipeline for a CSV upload.

        Returns:
            dict with keys: raw_count, cleaned_count, transaction_count, product_count
            or None on failure.
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # --- Step 1: Truncate both tables ---
            cursor.execute("TRUNCATE TABLE Row_data")
            cursor.execute("TRUNCATE TABLE Cleaned_data")
            conn.commit()
            print("[Pipeline] Truncated Row_data and Cleaned_data")

            # --- Step 2: Load CSV and insert into Row_data ---
            df = pd.read_csv(csv_path, encoding='utf-8-sig', low_memory=False)
            print(f"[Pipeline] Loaded CSV: {len(df)} rows, columns: {list(df.columns)}")

            raw_count = self._insert_into_row_data(cursor, df)
            conn.commit()
            print(f"[Pipeline] Inserted {raw_count} rows into Row_data")

            # --- Step 3: Copy Row_data -> Cleaned_data ---
            cursor.execute("INSERT INTO Cleaned_data SELECT * FROM Row_data")
            conn.commit()
            print("[Pipeline] Copied Row_data into Cleaned_data")

            # --- Step 4: Apply cleaning rules on Cleaned_data ---
            cleaned_count = self._clean_data(cursor)
            conn.commit()
            print(f"[Pipeline] Cleaned_data has {cleaned_count} rows after cleaning")

            # --- Step 5: Get stats ---
            cursor.execute("SELECT COUNT(DISTINCT InvoiceNo) FROM Cleaned_data")
            transaction_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(DISTINCT Description) FROM Cleaned_data WHERE Description IS NOT NULL")
            product_count = cursor.fetchone()[0]

            return {
                'raw_count': raw_count,
                'cleaned_count': cleaned_count,
                'transaction_count': transaction_count,
                'product_count': product_count,
            }

        except Exception as e:
            conn.rollback()
            print(f"[Pipeline] Error: {e}")
            raise

    # ------------------------------------------------------------------
    # Step 2 helper: bulk insert CSV rows into Row_data
    # ------------------------------------------------------------------
    def _insert_into_row_data(self, cursor, df):
        """Insert pandas DataFrame rows into Row_data in batches.

        All columns except UnitPrice are VARCHAR in SQL Server, so we cast
        everything to Python str (or None).  UnitPrice is DECIMAL(28,4) so
        we keep it as a Python float.
        """
        columns = [
            'InvoiceNo', 'StockCode', 'Description', 'Quantity',
            'InvoiceDate', 'UnitPrice', 'CustomerID', 'Country'
        ]

        # Map CSV column names to expected names (handles BOM, case diffs)
        col_map = {}
        for expected in columns:
            for actual in df.columns:
                if actual.strip().lower() == expected.lower():
                    col_map[expected] = actual
                    break

        # Build a clean DataFrame with native Python types
        df_insert = pd.DataFrame()
        for col in columns:
            if col in col_map:
                df_insert[col] = df[col_map[col]]
            else:
                df_insert[col] = None

        # Convert each column to the right Python type for pyodbc
        # VARCHAR columns -> str or None
        varchar_cols = ['InvoiceNo', 'StockCode', 'Description', 'Quantity',
                        'InvoiceDate', 'CustomerID', 'Country']
        for col in varchar_cols:
            df_insert[col] = df_insert[col].apply(
                lambda x: str(x).strip() if pd.notna(x) else None
            )

        # UnitPrice -> Python float or None (for DECIMAL(28,4))
        df_insert['UnitPrice'] = pd.to_numeric(df_insert['UnitPrice'], errors='coerce')
        df_insert['UnitPrice'] = df_insert['UnitPrice'].apply(
            lambda x: round(float(x), 4) if pd.notna(x) else None
        )

        sql = (
            "INSERT INTO Row_data (InvoiceNo, StockCode, Description, Quantity, "
            "InvoiceDate, UnitPrice, CustomerID, Country) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        )

        # Convert to list of tuples with native Python types
        rows = [tuple(row) for row in df_insert.itertuples(index=False, name=None)]

        # Insert in batches of 5000 to avoid memory issues
        batch_size = 5000
        cursor.fast_executemany = True
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            cursor.executemany(sql, batch)
            print(f"[Pipeline]   Inserted batch {i // batch_size + 1} "
                  f"({min(i + batch_size, len(rows))}/{len(rows)} rows)")

        return len(rows)

    # ------------------------------------------------------------------
    # Step 4 helper: apply cleaning rules
    # ------------------------------------------------------------------
    def _clean_data(self, cursor):
        """Apply the cleaning DELETE rules on Cleaned_data. Returns remaining row count."""
        cleaning_queries = [
            "DELETE FROM Cleaned_data WHERE InvoiceNo LIKE '%C%'",
            "DELETE FROM Cleaned_data WHERE CustomerID IS NULL",
            "DELETE FROM Cleaned_data WHERE Quantity LIKE '%-%'",
            "DELETE FROM Cleaned_data WHERE UnitPrice = 0 OR UnitPrice LIKE '%-%'",
            "DELETE FROM Cleaned_data WHERE Description IS NULL OR Description LIKE '%?%'",
            "DELETE FROM Cleaned_data WHERE StockCode = 'POST'",
        ]

        for query in cleaning_queries:
            cursor.execute(query)

        cursor.execute("SELECT COUNT(*) FROM Cleaned_data")
        return cursor.fetchone()[0]

    # ------------------------------------------------------------------
    # Transaction extraction (for mining)
    # ------------------------------------------------------------------
    def get_transactions(self):
        """Extract transactions from Cleaned_data grouped by InvoiceNo.

        Returns:
            list[list[str]] - each inner list is a deduplicated set of item descriptions
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        # Use STRING_AGG with DISTINCT via subquery for deduplication
        cursor.execute("""
            SELECT InvoiceNo, STRING_AGG(CAST(Description AS VARCHAR(MAX)), '||') AS Products
            FROM (
                SELECT DISTINCT InvoiceNo, Description
                FROM Cleaned_data
                WHERE Description IS NOT NULL
                  AND LTRIM(RTRIM(Description)) <> ''
            ) AS DistinctItems
            GROUP BY InvoiceNo
            ORDER BY InvoiceNo
        """)

        transactions = []
        for row in cursor.fetchall():
            items = [item.strip() for item in row[1].split('||') if item.strip()]
            if len(items) >= 2:  # Need at least 2 items for association rules
                transactions.append(items)

        print(f"[Pipeline] Extracted {len(transactions)} transactions (with 2+ items)")
        return transactions

    def get_transaction_stats(self):
        """Get summary statistics from Cleaned_data."""
        conn = self._get_connection()
        cursor = conn.cursor()

        stats = {}

        cursor.execute("SELECT COUNT(*) FROM Row_data")
        stats['raw_row_count'] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM Cleaned_data")
        stats['cleaned_row_count'] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(DISTINCT InvoiceNo) FROM Cleaned_data")
        stats['transaction_count'] = cursor.fetchone()[0]

        cursor.execute(
            "SELECT COUNT(DISTINCT Description) FROM Cleaned_data "
            "WHERE Description IS NOT NULL"
        )
        stats['unique_products'] = cursor.fetchone()[0]

        # Average basket size
        cursor.execute("""
            SELECT AVG(CAST(item_count AS FLOAT)) FROM (
                SELECT InvoiceNo, COUNT(DISTINCT Description) AS item_count
                FROM Cleaned_data
                WHERE Description IS NOT NULL
                GROUP BY InvoiceNo
            ) AS basket_sizes
        """)
        result = cursor.fetchone()[0]
        stats['avg_basket_size'] = round(result, 2) if result else 0

        return stats

    # ------------------------------------------------------------------
    # Product extraction (for Flask product catalog)
    # ------------------------------------------------------------------
    def get_unique_products(self):
        """Get unique products with average prices from Cleaned_data.

        Returns:
            list[dict] with keys: description, stock_code, avg_price
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                Description,
                MIN(StockCode) AS StockCode,
                AVG(CAST(UnitPrice AS FLOAT)) AS AvgPrice
            FROM Cleaned_data
            WHERE Description IS NOT NULL
              AND LTRIM(RTRIM(Description)) <> ''
              AND UnitPrice > 0
            GROUP BY Description
            ORDER BY Description
        """)

        products = []
        for row in cursor.fetchall():
            products.append({
                'description': row[0],
                'stock_code': row[1],
                'avg_price': round(float(row[2]), 2),
            })

        return products

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------
    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def __del__(self):
        self.close()
