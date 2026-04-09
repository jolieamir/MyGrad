"""
Application Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~
All settings are read from environment variables with sensible defaults.
Copy .env.example to .env and edit your credentials there.
"""
import os
from datetime import timedelta
from urllib.parse import quote_plus

basedir = os.path.abspath(os.path.dirname(__file__))

# Load .env file if python-dotenv is available
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(basedir, '.env'))
except ImportError:
    pass

# -----------------------------------------------------------
# Read SQL config from environment once (used by Config class)
# -----------------------------------------------------------
_SQL = {
    'driver':             os.environ.get('SQL_DRIVER', '{ODBC Driver 17 for SQL Server}'),
    'server':             os.environ.get('SQL_SERVER', '.'),
    'database':           os.environ.get('SQL_DATABASE', 'TestDB'),
    'trusted_connection': os.environ.get('SQL_TRUSTED_CONNECTION', 'yes'),
    'username':           os.environ.get('SQL_USERNAME', 'sa'),
    'password':           os.environ.get('SQL_PASSWORD', ''),
}


def _build_pyodbc_conn_str():
    if _SQL['trusted_connection'].lower() == 'yes':
        return (
            f"DRIVER={_SQL['driver']};"
            f"SERVER={_SQL['server']};"
            f"DATABASE={_SQL['database']};"
            f"Trusted_Connection=yes;"
            f"Encrypt=yes;TrustServerCertificate=yes;"
        )
    return (
        f"DRIVER={_SQL['driver']};"
        f"SERVER={_SQL['server']};"
        f"DATABASE={_SQL['database']};"
        f"UID={_SQL['username']};"
        f"PWD={_SQL['password']};"
        f"Encrypt=yes;TrustServerCertificate=yes;"
    )


def _build_sqlalchemy_uri():
    if _SQL['trusted_connection'].lower() == 'yes':
        return (
            f"mssql+pyodbc://@{_SQL['server']}/{_SQL['database']}"
            f"?driver=ODBC+Driver+17+for+SQL+Server"
            f"&Trusted_Connection=yes&Encrypt=yes&TrustServerCertificate=yes"
        )
    pwd = quote_plus(_SQL['password'])
    return (
        f"mssql+pyodbc://{_SQL['username']}:{pwd}@{_SQL['server']}/{_SQL['database']}"
        f"?driver=ODBC+Driver+17+for+SQL+Server"
        f"&Encrypt=yes&TrustServerCertificate=yes"
    )


class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')

    # SQL Server
    SQL_SERVER_CONFIG = _SQL
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL') or _build_sqlalchemy_uri()
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # File upload
    UPLOAD_FOLDER = os.path.join(basedir, 'data', 'raw')
    MAX_CONTENT_LENGTH = 128 * 1024 * 1024  # 128 MB
    ALLOWED_EXTENSIONS = {'csv', 'txt'}

    # Session
    PERMANENT_SESSION_LIFETIME = timedelta(minutes=30)

    # Data Mining defaults
    DEFAULT_MIN_SUPPORT = 0.01
    DEFAULT_MIN_CONFIDENCE = 0.5
    MIN_SUPPORT_OPTIONS = [0.001, 0.005, 0.01, 0.02, 0.05, 0.1]
    MIN_CONFIDENCE_OPTIONS = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]

    @staticmethod
    def get_sql_server_connection_string():
        return _build_pyodbc_conn_str()
