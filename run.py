import os
from app import create_app, db
from app.models import User, Product, MiningResult, Order, OrderItem

# PySpark requires JAVA_HOME and explicit Python path (Windows Store alias workaround)
import sys
os.environ.setdefault("JAVA_HOME", r"C:\Program Files\Microsoft\jdk-17.0.18.8-hotspot")
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

app = create_app()


@app.shell_context_processor
def make_shell_context():
    return {
        'db': db,
        'User': User,
        'Product': Product,
        'MiningResult': MiningResult,
        'Order': Order,
        'OrderItem': OrderItem,
    }


@app.cli.command()
def init_db():
    """Initialize the SQLite database and create admin user."""
    with app.app_context():
        db.create_all()
        User.create_admin()
        print('Database initialized. Admin user created.')
        print('Login credentials: username=admin, password=admin123')


@app.cli.command()
def test_sql_server():
    """Test the SQL Server connection for the data pipeline."""
    from data_mining.data_pipeline import DataPipeline
    pipeline = DataPipeline()
    if pipeline.test_connection():
        print('SQL Server connection successful.')
        stats = pipeline.get_transaction_stats()
        for k, v in stats.items():
            print(f'  {k}: {v}')
    else:
        print('SQL Server connection FAILED. Check config.py / .env settings.')
    pipeline.close()


if __name__ == '__main__':
    with app.app_context():
        db.create_all()
        User.create_admin()

    app.run(debug=True, host='0.0.0.0', port=5000)
