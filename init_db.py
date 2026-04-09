"""Initialize the SQL Server database with tables and admin user.

Usage:
    python init_db.py

Requires:
  - SQL Server running with TestDB database
  - Tables created via sql/setup_tables.sql
  - ODBC Driver 17 for SQL Server installed

This script uses SQLAlchemy to create/sync Flask model tables and seed data.
"""
from app import create_app, db
from app.models import User, Product

app = create_app()

with app.app_context():
    # Create/sync tables via SQLAlchemy (safe if they already exist)
    db.create_all()

    # Create admin user
    User.create_admin()
    print("Admin user created (admin / admin123)")

    # Seed sample products if empty
    if Product.query.count() > 0:
        print(f"Products already exist ({Product.query.count()} found)")
    else:
        samples = [
            Product(name='Bread', category='Bakery', price=2.50,
                    stock_code='BRD001', description='Fresh white bread loaf'),
            Product(name='Jam', category='Condiments', price=3.00,
                    stock_code='JAM001', description='Strawberry jam jar'),
            Product(name='Pasta', category='Dry Goods', price=1.50,
                    stock_code='PST001', description='Spaghetti pasta 500g'),
            Product(name='Pasta Sauce', category='Condiments', price=2.75,
                    stock_code='SAC001', description='Tomato pasta sauce'),
            Product(name='Milk', category='Dairy', price=2.00,
                    stock_code='MLK001', description='Whole milk 1L'),
            Product(name='Butter', category='Dairy', price=3.50,
                    stock_code='BUT001', description='Salted butter 250g'),
            Product(name='Eggs', category='Dairy', price=4.00,
                    stock_code='EGG001', description='Dozen eggs'),
            Product(name='Coffee', category='Beverages', price=8.00,
                    stock_code='COF001', description='Ground coffee 250g'),
            Product(name='Cheese', category='Dairy', price=5.50,
                    stock_code='CHS001', description='Cheddar cheese block'),
            Product(name='Olive Oil', category='Oils', price=6.00,
                    stock_code='OIL001', description='Extra virgin olive oil'),
        ]
        for p in samples:
            db.session.add(p)
        db.session.commit()
        print(f"Added {len(samples)} sample products")

    print("\nDatabase ready.")
    print("Note: Data pipeline tables (Row_data, Cleaned_data) should be")
    print("created via sql/setup_tables.sql if not already present.")
