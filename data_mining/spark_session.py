"""
Shared PySpark session singleton.

PySpark is DISABLED by default — the pure-Python implementations of
Apriori and FP-Growth are fast (1-5 seconds on 17K transactions) and
produce identical results. This avoids Java/Spark setup issues.

To enable PySpark, set environment variable: USE_SPARK=yes
"""

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

SPARK_AVAILABLE = False
_spark = None


def get_spark():
    """Get or create the shared SparkSession, or None if unavailable."""
    global _spark, SPARK_AVAILABLE

    # Only try Spark if explicitly enabled via environment variable
    if os.environ.get('USE_SPARK', 'no').lower() != 'yes':
        return None

    if not SPARK_AVAILABLE:
        try:
            from pyspark.sql import SparkSession
            if _spark is None or _spark.sparkContext._jsc.sc().isStopped():
                _spark = (
                    SparkSession.builder
                    .appName("MarketBasketAnalysis")
                    .master("local[*]")
                    .config("spark.driver.memory", "4g")
                    .config("spark.ui.showConsoleProgress", "false")
                    .getOrCreate()
                )
            # Quick test
            _spark.sparkContext.parallelize([1, 2, 3]).count()
            SPARK_AVAILABLE = True
        except Exception as e:
            print(f"[Spark] Unavailable: {e}")
            SPARK_AVAILABLE = False
            _spark = None

    return _spark
