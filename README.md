# Market Basket Analysis Web Application

A complete e-commerce demo application powered by distributed data mining algorithms (Apriori and FP-Growth) for market basket analysis.

## 🎯 Project Overview

This web application demonstrates how distributed frequent itemset mining and association rule mining can power real-world retail recommendations. It consists of two interfaces:

1. **Customer App** - Shopping experience with intelligent product recommendations
2. **Manager App** - Data mining dashboard to run algorithms and manage products

## 🚀 Features

### Customer Features
- Browse products by category
- View product details with recommendations
- Shopping cart functionality
- Simulated checkout process
- "Frequently bought together" recommendations
- Product search

### Manager Features
- Upload transaction data (CSV)
- Run Apriori and FP-Growth algorithms
- Configure mining parameters (min_support, min_confidence)
- View and export mining results
- Manage product catalog
- View generated recommendations

## 🛠️ Technology Stack

- **Backend**: Flask (Python)
- **Data Mining**: PySpark (distributed Apriori & FP-Growth)
- **Database**: SQLite (web app) + SQL Server (transaction data)
- **Frontend**: HTML/CSS/JavaScript with Bootstrap
- **Visualization**: Plotly/Chart.js

## 📁 Project Structure

```
market-basket-mining/
├── app/
│   ├── __init__.py              # Flask app factory
│   ├── models.py                # Database models
│   ├── customer_app/            # Customer interface
│   ├── manager_app/             # Manager interface
│   └── api/                     # API endpoints
├── data_mining/
│   ├── __init__.py
│   ├── legacy_code.py           # Original code (preserved)
│   ├── apriori.py               # Apriori algorithm
│   ├── fpgrowth.py              # FP-Growth algorithm
│   ├── recommendations.py       # Recommendation engine
│   └── data_pipeline.py         # Data processing
├── templates/                   # HTML templates
├── config.py                    # Configuration
├── run.py                       # Application entry point
└── requirements.txt             # Dependencies
```

## 🚀 Quick Start

### Prerequisites
- Python 3.11+
- Java JDK 17 (for PySpark)
- SQL Server (optional, for transaction data)

### Installation

1. **Clone or create the project directory**
   ```bash
   cd market-basket-mining
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   ```

3. **Activate virtual environment**
   - Windows: `venv\Scripts\activate`
   - Linux/Mac: `source venv/bin/activate`

4. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

5. **Initialize database and add sample products**
   ```bash
   python run.py
   ```
   This will:
   - Create SQLite database
   - Create admin user (username: `admin`, password: `admin123`)
   - Add sample products

 6. **Run the application**
    ```bash
    python run.py
    ```
    
 7. **Access the application**
    - Customer App: http://localhost:5000/ or http://127.0.0.1:5000/
    - Manager App: http://localhost:5000/admin/ or http://127.0.0.1:5000/admin/
    - Login credentials for manager: username=admin, password=admin123

 8. **Upload your CSV data** (Optional)
    - Go to Manager App: http://localhost:5000/admin/
    - Login with admin/admin123
    - Click "Upload Data" and select your CSV file
    - Process the file to replace sample products with your real data
    - Your CSV should have columns: InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country
    - Large files (44MB+) may take 1-2 minutes to process

## 📖 Usage Guide

### For Customers

1. **Browse Products**: Visit http://localhost:5000/ to see the product catalog
2. **View Details**: Click on any product to see recommendations
3. **Add to Cart**: Click "Add to Cart" to build your shopping basket
4. **Checkout**: Go to cart and complete the simulated purchase

### For Managers

1. **Login**: Go to http://localhost:5000/admin/ and login with `admin`/`admin123`
2. **Upload Data**: Upload a CSV file with transaction data
3. **Run Mining**: Select algorithm (Apriori or FP-Growth) and set parameters
4. **View Results**: See frequent itemsets and association rules
5. **Export**: Download results as JSON or CSV

### Recommended Mining Parameters

For your dataset (OnlineRetail.csv with ~18,480 transactions):

| Parameter | Recommended Value | Description |
|-----------|------------------|-------------|
| **Algorithm** | FP-Growth | Faster for large datasets |
| **Min Support** | 0.002 (0.2%) | Patterns appearing in at least 37 transactions |
| **Min Confidence** | 0.3 (30%) | Rules with at least 30% confidence |

**Adjusting Parameters:**
- **Lower support** (0.001-0.002): More patterns, longer processing time
- **Higher support** (0.01-0.05): Fewer but stronger patterns  
- **Lower confidence** (0.1-0.3): More association rules
- **Higher confidence** (0.5-0.8): Stronger but fewer rules

**Getting More Results:**
If you're getting too few patterns, try:
- Reduce min_support to 0.001 or 0.002
- Reduce min_confidence to 0.2 or 0.3
- Use FP-Growth algorithm (better for finding more patterns)

## 🔧 Configuration

Edit `config.py` to customize:

```python
# Database - SQL Server
SQLALCHEMY_DATABASE_URI = 'mssql+pyodbc://localhost/TestDB?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'

# File Upload
UPLOAD_FOLDER = 'data/raw'
MAX_CONTENT_LENGTH = 128 * 1024 * 1024  # 128MB

# PySpark
SPARK_APP_NAME = "MarketBasketAnalysis"
SPARK_MASTER = "local[*]"

# Data Mining Defaults
DEFAULT_MIN_SUPPORT = 0.01
DEFAULT_MIN_CONFIDENCE = 0.5
```

## 🗄️ Database Setup

### SQL Server (Web App)
The application uses SQL Server for all data storage. Create the database first:

```sql
CREATE DATABASE TestDB;
```

The application will automatically create the following tables:
- Users (admin)
- Products
- Recommendations
- Mining Results
- Orders

### SQL Server Configuration
Configure in `config.py`:

```python
SQL_SERVER_CONFIG = {
    'driver': '{ODBC Driver 17 for SQL Server}',
    'server': 'localhost',
    'database': 'TestDB',
    'trusted_connection': 'yes'
}
```

## 📊 Data Mining Algorithms

### FP-Growth (Distributed)
- **Approach**: PySpark ML implementation using distributed computing
- **Efficiency**: Faster for large datasets, tree-based approach
- **Use Case**: Primary algorithm for recommendations
- **Features**: Generates both frequent itemsets and association rules

### Apriori (SON - Distributed)
- **Approach**: SON (Savasere-Omiecinski-Navathe) algorithm using PySpark
- **Efficiency**: Good for validation, distributed candidate generation
- **Use Case**: Cross-checking FP-Growth results
- **Features**: Two-phase algorithm (local candidates + global counting)

## 🔌 API Endpoints

- `GET /api/products` - List all products
- `GET /api/products/<id>` - Get product details
- `GET /api/products/<id>/recommendations` - Get recommendations
- `GET /api/mining/results` - Get recent mining results

## 🧪 Testing

Run the application and test:

1. **Customer Flow**:
   - Browse products
   - Add items to cart
   - Complete checkout

2. **Manager Flow**:
   - Login to admin
   - Upload sample data
   - Run mining algorithms
   - View results

## 📝 Sample Data

The application includes sample products:
- Bread, Jam, Butter, Milk, Eggs, Cheese
- Pasta, Pasta Sauce, Olive Oil
- Candy, Gum
- Coffee, Soda
- Honey, BBQ Sauce

## 🐛 Troubleshooting

### Java Not Found
```bash
# Set JAVA_HOME
set JAVA_HOME=C:\Program Files\Java\jdk-17  # Windows
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk  # Linux
```

### Port Already in Use
```python
# Edit run.py
app.run(debug=True, host='0.0.0.0', port=5001)  # Change port
```

### Database Locked
```bash
# Delete and recreate
rm app.db  # Linux/Mac
del app.db  # Windows
python run.py
```

## 📚 References

This project is based on the thesis:
"Distributed Apriori and FP-Growth Mining for Efficient Market Basket Analysis"
by Jolie Amir Habib, supervised by Prof. Mona Farouk

## 🤝 Contributing

This is an academic project. For questions or suggestions, please contact the author.

## 📄 License

This project is for educational and demonstration purposes.

## 🙏 Acknowledgments

- Prof. Mona Farouk (Supervisor)
- October University for Modern Science & Arts (MSA)
- Industrial partner: Mr. Mohammed El-Sayed (Spinneys)

---

**Built with ❤️ using Flask and PySpark**#   M y G r a d  
 