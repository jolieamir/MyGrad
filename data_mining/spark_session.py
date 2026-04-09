"""
Shared PySpark session singleton.

On Python 3.13+ / Windows, PySpark workers crash. We detect this once
at import time and set SPARK_AVAILABLE accordingly. When False, miners
use pure-Python fallback implementations.
"""

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Python 3.13+ on Windows: PySpark workers are known to crash.
# Skip the expensive Spark test entirely in that case.
_py_version = sys.version_info
SPARK_AVAILABLE = False

if _py_version < (3, 13):
    try:
        from pyspark.sql import SparkSession
        SPARK_AVAILABLE = True
    except ImportError:
        SPARK_AVAILABLE = False
else:
    print(f"[Spark] Python {_py_version.major}.{_py_version.minor} detected — "
          f"PySpark workers incompatible, using pure-Python algorithms")

_spark = None


def get_spark():
    """Get or create the shared SparkSession, or None if unavailable."""
    global _spark
    if not SPARK_AVAILABLE:
        return None

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
        return _spark
    except Exception:
        return None
