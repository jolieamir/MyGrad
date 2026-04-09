"""Smoke test for the entire MyGrad application."""
import sys
import json
import os

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

PASS = 0
FAIL = 0


def check(label, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  PASS: {label}" + (f" ({detail})" if detail else ""))
    else:
        FAIL += 1
        print(f"  FAIL: {label}" + (f" ({detail})" if detail else ""))


print("=" * 60)
print("SMOKE TEST: MyGrad Application")
print("=" * 60)

# ============================================================
# 1. Config & SQL Server Connection
# ============================================================
print("\n[1/8] Config & SQL Server Connection")
try:
    from config import Config
    import pyodbc

    conn_str = Config.get_sql_server_connection_string()
    conn = pyodbc.connect(conn_str, timeout=10)
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    check("SQL Server connection", cursor.fetchone()[0] == 1)

    tables_ok = True
    for table in ["Row_data", "Cleaned_data", "tableed", "FPResults"]:
        cursor.execute(f"SELECT OBJECT_ID('{table}', 'U')")
        if cursor.fetchone()[0] is None:
            tables_ok = False
            print(f"    MISSING: {table}")
    check("Pipeline tables exist", tables_ok)
    conn.close()
except Exception as e:
    check("SQL Server connection", False, str(e))

# ============================================================
# 2. Flask App & Models
# ============================================================
print("\n[2/8] Flask App & Models")
try:
    from app import create_app, db
    from app.models import User, Product, MiningResult, Recommendation, Order, OrderItem

    app = create_app()
    with app.app_context():
        db.create_all()
        admin = User.query.filter_by(username="admin").first()
        check("Admin user exists", admin is not None)
        if admin:
            check("Admin password valid", admin.check_password("admin123"))
        product_count = Product.query.count()
        check("Products in catalog", product_count > 0, f"{product_count} products")
except Exception as e:
    check("Flask app creation", False, str(e))

# ============================================================
# 3. Data Pipeline
# ============================================================
print("\n[3/8] Data Pipeline (SQL Server)")
try:
    from data_mining.data_pipeline import DataPipeline

    pipeline = DataPipeline()
    check("Pipeline connection", pipeline.test_connection())

    stats = pipeline.get_transaction_stats()
    has_data = stats.get("cleaned_row_count", 0) > 0
    if has_data:
        check("Data loaded in Cleaned_data", True,
              f"{stats['cleaned_row_count']} rows, {stats['transaction_count']} txns, "
              f"{stats['unique_products']} products, avg basket {stats['avg_basket_size']}")
    else:
        print("  INFO: No data loaded. Running pipeline with OnlineRetail.csv...")
        result = pipeline.run_pipeline("OnlineRetail.csv")
        check("Pipeline execution", result is not None,
              f"raw={result['raw_count']}, cleaned={result['cleaned_count']}, "
              f"txns={result['transaction_count']}")
        # Sync products
        with app.app_context():
            unique_products = pipeline.get_unique_products()
            Recommendation.query.delete()
            Product.query.delete()
            db.session.commit()
            for prod in unique_products:
                p = Product(
                    name=prod["description"], category="General",
                    price=prod["avg_price"], description=prod["description"],
                    stock_code=prod["stock_code"],
                )
                db.session.add(p)
            db.session.commit()
            check("Products synced to catalog", True, f"{len(unique_products)} products")
except Exception as e:
    check("Data pipeline", False, str(e))

# ============================================================
# 4. Transaction Extraction
# ============================================================
print("\n[4/8] Transaction Extraction")
try:
    transactions = pipeline.get_transactions()
    check("Transactions extracted", len(transactions) > 0, f"{len(transactions)} transactions")

    # Verify dedup
    dedup_ok = all(len(t) == len(set(t)) for t in transactions[:100])
    check("No duplicate items per transaction", dedup_ok)

    # Verify min 2 items
    min2_ok = all(len(t) >= 2 for t in transactions)
    check("All transactions have 2+ items", min2_ok)
except Exception as e:
    check("Transaction extraction", False, str(e))

# ============================================================
# 5. Unique Products
# ============================================================
print("\n[5/8] Product Extraction from Cleaned_data")
try:
    products = pipeline.get_unique_products()
    check("Products extracted", len(products) > 0, f"{len(products)} products")
    sample = products[0]
    check("Product has description/stock_code/avg_price",
          all(k in sample for k in ["description", "stock_code", "avg_price"]))
    pipeline.close()
except Exception as e:
    check("Product extraction", False, str(e))

# ============================================================
# 6. FP-Growth
# ============================================================
print("\n[6/8] FP-Growth Algorithm")
try:
    from data_mining.fpgrowth import FPGrowthMiner

    test_txns = transactions[:2000]
    fp = FPGrowthMiner(min_support=0.02, min_confidence=0.3)
    fp_results = fp.run_distributed(test_txns)

    check("Returns frequent_itemsets", "frequent_itemsets" in fp_results)
    check("Returns association_rules", "association_rules" in fp_results)

    n_is = len(fp_results["frequent_itemsets"])
    n_rules = len(fp_results["association_rules"])
    check("Found frequent itemsets", n_is > 0, f"{n_is} itemsets")
    check("Found association rules", n_rules > 0, f"{n_rules} rules")

    if n_is > 0:
        s = fp_results["frequent_itemsets"][0]
        check("Itemset has items/freq/support",
              all(k in s for k in ["items", "freq", "support"]))

    if n_rules > 0:
        r = fp_results["association_rules"][0]
        check("Rule has antecedents/consequents/confidence/support/lift",
              all(k in r for k in ["antecedents", "consequents", "confidence", "support", "lift"]))
        print(f"    Top rule: {r['antecedents']} -> {r['consequents']} "
              f"conf={r['confidence']}, lift={r['lift']}")
except Exception as e:
    check("FP-Growth", False, str(e))

# ============================================================
# 7. Apriori
# ============================================================
print("\n[7/8] Apriori Algorithm")
try:
    from data_mining.apriori import AprioriMiner

    ap = AprioriMiner(min_support=0.02, min_confidence=0.3)
    ap_results = ap.run_distributed(test_txns)

    n_is = len(ap_results["frequent_itemsets"])
    n_rules = len(ap_results["association_rules"])
    check("Found frequent itemsets", n_is > 0, f"{n_is} itemsets")
    check("Found association rules", n_rules > 0, f"{n_rules} rules")

    if n_rules > 0:
        r = ap_results["association_rules"][0]
        print(f"    Top rule: {r['antecedents']} -> {r['consequents']} "
              f"conf={r['confidence']}, lift={r['lift']}")
except Exception as e:
    check("Apriori", False, str(e))

# ============================================================
# 8. Recommendation Engine
# ============================================================
print("\n[8/8] Recommendation Engine")
try:
    from data_mining.recommendations import RecommendationEngine

    engine = RecommendationEngine()
    engine.load_mining_results(
        fp_results["frequent_itemsets"], fp_results["association_rules"]
    )

    n_recs = len(engine.recommendations)
    check("Recommendations generated", n_recs > 0, f"{n_recs} products have recs")

    sample_product = list(engine.recommendations.keys())[0]
    recs = engine.get_recommendations(sample_product, top_n=3)
    check("get_recommendations works", len(recs) > 0)
    check("Rec has product/confidence/support",
          all(k in recs[0] for k in ["product", "confidence", "support"]))
    print(f'    "{sample_product}" -> {[r["product"] for r in recs[:2]]}')

    # Cross-sell
    cart = list(engine.recommendations.keys())[:2]
    cross = engine.get_cross_sell(cart, top_n=3)
    check("get_cross_sell works", isinstance(cross, list))

    # Bundles
    bundles = engine.get_bundles(min_items=2, max_items=3, top_n=5)
    check("get_bundles works", isinstance(bundles, list), f"{len(bundles)} bundles")

    # Serialization
    json_str = engine.to_json()
    check("to_json works", len(json_str) > 10)
except Exception as e:
    check("Recommendation engine", False, str(e))

# ============================================================
# BONUS: Flask HTTP Routes
# ============================================================
print("\n[BONUS] Flask HTTP Routes")
try:
    with app.test_client() as client:
        # Customer pages
        r = client.get("/")
        check("GET / (home)", r.status_code == 200)

        r = client.get("/search?q=test")
        check("GET /search", r.status_code == 200)

        # Manager login
        r = client.get("/admin/login")
        check("GET /admin/login", r.status_code == 200)

        r = client.post("/admin/login",
                        data={"username": "admin", "password": "admin123"},
                        follow_redirects=True)
        check("POST /admin/login (auth)", r.status_code == 200)

        # Manager pages (now logged in)
        for route in ["/admin/dashboard", "/admin/mining", "/admin/products",
                      "/admin/recommendations", "/admin/upload"]:
            r = client.get(route)
            check(f"GET {route}", r.status_code == 200)

        # API endpoints
        r = client.get("/api/products")
        check("GET /api/products", r.status_code == 200)
        data = json.loads(r.data)
        check("API returns product list", isinstance(data, list), f"{len(data)} products")

        r = client.get("/api/mining/results")
        check("GET /api/mining/results", r.status_code == 200)

        # Cart operations
        if data:
            pid = data[0]["id"]
            r = client.post(f"/cart/add/{pid}", follow_redirects=True)
            check("POST /cart/add", r.status_code == 200)

            r = client.get("/cart")
            check("GET /cart", r.status_code == 200)
except Exception as e:
    check("Flask routes", False, str(e))

# ============================================================
# Summary
# ============================================================
print("\n" + "=" * 60)
total = PASS + FAIL
print(f"RESULTS: {PASS}/{total} passed, {FAIL} failed")
if FAIL == 0:
    print("ALL TESTS PASSED")
else:
    print(f"FAILURES: {FAIL}")
print("=" * 60)

sys.exit(0 if FAIL == 0 else 1)
