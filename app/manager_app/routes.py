from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify
from flask_login import login_user, logout_user, login_required, current_user
from werkzeug.utils import secure_filename
from app import db
from app.models import User, Product, MiningResult, Recommendation
from config import Config
import os
import json
import threading
from datetime import datetime

manager_bp = Blueprint('manager_app', __name__)

ALLOWED_EXTENSIONS = {'csv', 'txt'}

# Global mining progress tracker
_mining_progress = {
    'running': False,
    'step': 0,
    'total_steps': 5,
    'message': '',
    'detail': '',
    'sub_progress': 0,  # 0-100 within current step
    'done': False,
    'error': None,
    'result_id': None,
}


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


# ======================================================================
# Auth
# ======================================================================

@manager_bp.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('manager_app.dashboard'))

    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        user = User.query.filter_by(username=username).first()

        if user and user.check_password(password):
            login_user(user)
            return redirect(url_for('manager_app.dashboard'))

        flash('Invalid username or password', 'error')

    return render_template('manager_app/login.html')


@manager_bp.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('manager_app.login'))


# ======================================================================
# Dashboard
# ======================================================================

@manager_bp.route('/')
@manager_bp.route('/dashboard')
@login_required
def dashboard():
    product_count = Product.query.count()
    recommendation_count = Recommendation.query.count()
    recent_results = MiningResult.query.order_by(MiningResult.created_at.desc()).limit(5).all()

    # Try to get SQL Server stats
    sql_stats = {}
    try:
        from data_mining.data_pipeline import DataPipeline
        pipeline = DataPipeline()
        if pipeline.test_connection():
            sql_stats = pipeline.get_transaction_stats()
        pipeline.close()
    except Exception:
        pass

    return render_template('manager_app/dashboard.html',
                           product_count=product_count,
                           recommendation_count=recommendation_count,
                           recent_results=recent_results,
                           sql_stats=sql_stats)


# ======================================================================
# CSV Upload  ->  full SQL Server pipeline
# ======================================================================

@manager_bp.route('/upload', methods=['GET', 'POST'])
@login_required
def upload():
    if request.method == 'POST':
        if 'file' not in request.files:
            flash('No file selected', 'error')
            return redirect(request.url)

        file = request.files['file']
        if file.filename == '':
            flash('No file selected', 'error')
            return redirect(request.url)

        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            upload_dir = Config.UPLOAD_FOLDER
            os.makedirs(upload_dir, exist_ok=True)
            filepath = os.path.join(upload_dir, filename)
            file.save(filepath)

            # Store path for the processing step
            session['uploaded_file'] = filepath
            flash(f'File {filename} uploaded. Click "Process" to run the data pipeline.', 'success')
            return redirect(url_for('manager_app.process_upload'))

        flash('File type not allowed. Use .csv or .txt', 'error')

    return render_template('manager_app/upload.html')


@manager_bp.route('/process_upload', methods=['GET', 'POST'])
@login_required
def process_upload():
    filepath = session.get('uploaded_file')

    if not filepath or not os.path.exists(filepath):
        flash('No file to process. Please upload a CSV first.', 'error')
        return redirect(url_for('manager_app.upload'))

    if request.method == 'POST':
        try:
            from data_mining.data_pipeline import DataPipeline

            pipeline = DataPipeline()

            # ---- Run the full pipeline ----
            # 1. TRUNCATE Row_data & Cleaned_data
            # 2. Insert raw CSV -> Row_data
            # 3. Copy Row_data -> Cleaned_data
            # 4. Apply cleaning DELETEs on Cleaned_data
            stats = pipeline.run_pipeline(filepath)

            # ---- Sync products into Flask catalog ----
            unique_products = pipeline.get_unique_products()

            # Clear old products and recommendations
            Recommendation.query.delete()
            Product.query.delete()
            db.session.commit()

            for prod in unique_products:
                product = Product(
                    name=prod['description'],
                    category='General',
                    price=prod['avg_price'],
                    description=prod['description'],
                    stock_code=prod['stock_code'],
                )
                db.session.add(product)

            db.session.commit()
            pipeline.close()

            flash(
                f"Pipeline complete! "
                f"Raw rows: {stats['raw_count']}, "
                f"Cleaned rows: {stats['cleaned_count']}, "
                f"Transactions: {stats['transaction_count']}, "
                f"Products: {stats['product_count']}",
                'success',
            )
            session.pop('uploaded_file', None)

        except Exception as e:
            flash(f'Pipeline error: {str(e)}', 'error')

        return redirect(url_for('manager_app.dashboard'))

    return render_template('manager_app/process_upload.html', filepath=filepath)


# ======================================================================
# Mining  ->  reads transactions from Cleaned_data in SQL Server
# ======================================================================

@manager_bp.route('/mining', methods=['GET'])
@login_required
def mining():
    return render_template('manager_app/mining.html',
                           min_support_options=Config.MIN_SUPPORT_OPTIONS,
                           min_confidence_options=Config.MIN_CONFIDENCE_OPTIONS)


@manager_bp.route('/mining/run', methods=['POST'])
@login_required
def mining_run():
    """Start mining in a background thread. Returns JSON immediately."""
    global _mining_progress

    if _mining_progress['running']:
        return jsonify({'error': 'Mining already in progress'}), 409

    algorithm = request.form.get('algorithm', 'fpgrowth')
    min_support = float(request.form.get('min_support', 0.01))
    min_confidence = float(request.form.get('min_confidence', 0.5))
    username = current_user.username

    # Reset progress
    _mining_progress = {
        'running': True, 'step': 0, 'total_steps': 5,
        'message': 'Starting...', 'detail': '', 'done': False,
        'error': None, 'result_id': None,
    }

    def _run_mining(app, algorithm, min_support, min_confidence, username):
        global _mining_progress
        p = _mining_progress

        try:
            with app.app_context():
                # Progress callback for algorithms
                def on_progress(pct, detail=None):
                    p['sub_progress'] = pct
                    if detail:
                        p['detail'] = detail

                # Step 1: Load transactions
                p.update(step=1, sub_progress=0,
                         message='Loading transactions from SQL Server...',
                         detail='Querying Cleaned_data and grouping by InvoiceNo')

                from data_mining.data_pipeline import DataPipeline
                from data_mining.fpgrowth import FPGrowthMiner
                from data_mining.apriori import AprioriMiner
                from data_mining.recommendations import RecommendationEngine

                pipeline = DataPipeline()
                p['sub_progress'] = 50
                transactions = pipeline.get_transactions()
                pipeline.close()

                if not transactions:
                    p.update(running=False, done=True,
                             error='No transactions found. Upload a CSV first.')
                    return

                p.update(sub_progress=100,
                         detail=f'{len(transactions)} transactions loaded')

                # Step 2: Run algorithm
                p.update(step=2, sub_progress=0,
                         message=f'Running {algorithm.upper()} algorithm...',
                         detail=f'Processing {len(transactions)} transactions '
                                f'(support={min_support}, confidence={min_confidence})')

                if algorithm == 'fpgrowth':
                    miner = FPGrowthMiner(min_support=min_support, min_confidence=min_confidence)
                else:
                    miner = AprioriMiner(min_support=min_support, min_confidence=min_confidence)

                results = miner.run_distributed(transactions, progress_callback=on_progress)
                miner.close()

                n_itemsets = len(results.get('frequent_itemsets', []))
                n_rules = len(results.get('association_rules', []))
                p.update(sub_progress=100,
                         detail=f'Found {n_itemsets} itemsets, {n_rules} rules')

                # Step 3: Save results
                p.update(step=3, sub_progress=0,
                         message='Saving mining results...',
                         detail=f'{n_itemsets} itemsets, {n_rules} rules')

                mining_result = MiningResult(
                    algorithm=algorithm,
                    parameters=json.dumps({
                        'min_support': min_support,
                        'min_confidence': min_confidence,
                        'transaction_count': len(transactions),
                    }),
                    results=json.dumps(results),
                    created_by=username,
                )
                db.session.add(mining_result)
                db.session.commit()
                p['sub_progress'] = 100

                # Step 4: Generate recommendations
                p.update(step=4, sub_progress=0,
                         message='Generating product recommendations...',
                         detail='Building recommendation lookup from rules')

                rec_engine = RecommendationEngine()
                rec_engine.load_mining_results(
                    results.get('frequent_itemsets', []),
                    results.get('association_rules', []),
                )
                p['sub_progress'] = 50

                Recommendation.query.filter_by(algorithm=algorithm).delete()
                db.session.commit()
                p['sub_progress'] = 100

                # Step 5: Save recommendations
                total_products = len(rec_engine.recommendations)
                p.update(step=5, sub_progress=0,
                         message=f'Saving {algorithm.upper()} recommendations...',
                         detail=f'0 / {total_products} products')

                saved_count = 0
                for i, (product_name, recs) in enumerate(rec_engine.recommendations.items()):
                    product = Product.query.filter_by(name=product_name).first()
                    if not product:
                        continue
                    for rec in recs[:10]:
                        rec_product = Product.query.filter_by(name=rec['product']).first()
                        if not rec_product:
                            continue
                        db.session.add(Recommendation(
                            product_id=product.id,
                            recommended_with_id=rec_product.id,
                            confidence=rec.get('confidence', 0),
                            support=rec.get('support', 0),
                            lift=rec.get('lift', 0),
                            algorithm=algorithm,
                        ))
                        saved_count += 1

                    # Update sub-progress every 50 products
                    if i % 50 == 0 or i == total_products - 1:
                        pct = int((i + 1) / total_products * 100)
                        p.update(sub_progress=pct,
                                 detail=f'{i + 1} / {total_products} products '
                                        f'({saved_count} recs)')

                db.session.commit()

                p.update(
                    running=False, done=True, error=None,
                    result_id=mining_result.id,
                    message='Mining complete!',
                    detail=f'{n_itemsets} itemsets, {n_rules} rules, '
                           f'{saved_count} recommendations saved',
                )

        except Exception as e:
            _mining_progress.update(running=False, done=True, error=str(e))

    # Launch in background thread
    from flask import current_app
    app = current_app._get_current_object()
    thread = threading.Thread(
        target=_run_mining,
        args=(app, algorithm, min_support, min_confidence, username),
        daemon=True,
    )
    thread.start()

    return jsonify({'status': 'started'})


@manager_bp.route('/mining/status')
@login_required
def mining_status():
    """Poll this endpoint for mining progress."""
    return jsonify(_mining_progress)


# ======================================================================
# Results
# ======================================================================

@manager_bp.route('/results')
@login_required
def results():
    result_id = request.args.get('result_id')

    if result_id:
        result = MiningResult.query.get_or_404(result_id)
        # Pre-parse JSON so the template gets dicts (not strings)
        parsed_params = json.loads(result.parameters) if result.parameters else {}
        parsed_data = json.loads(result.results) if result.results else {}
        return render_template('manager_app/results.html', result=result,
                               parsed_params=parsed_params, parsed_data=parsed_data)

    all_results = MiningResult.query.order_by(MiningResult.created_at.desc()).all()
    return render_template('manager_app/results.html', results=all_results)


@manager_bp.route('/results/<int:result_id>/export')
@login_required
def export_results(result_id):
    result = MiningResult.query.get_or_404(result_id)
    format_type = request.args.get('format', 'json')

    if format_type == 'json':
        response_data = {
            'algorithm': result.algorithm,
            'parameters': json.loads(result.parameters),
            'results': json.loads(result.results),
            'created_at': result.created_at.isoformat(),
        }
        return jsonify(response_data)

    elif format_type == 'csv':
        results_data = json.loads(result.results)
        lines = ['items,freq,support']
        for itemset in results_data.get('frequent_itemsets', []):
            items = ' | '.join(itemset['items'])
            freq = itemset.get('freq', 0)
            support = itemset.get('support', 0)
            lines.append(f'"{items}",{freq},{support}')
        csv_content = '\n'.join(lines)
        return csv_content, 200, {
            'Content-Type': 'text/csv',
            'Content-Disposition': f'attachment; filename=mining_results_{result_id}.csv',
        }

    flash('Invalid export format', 'error')
    return redirect(url_for('manager_app.results'))


# ======================================================================
# Product CRUD
# ======================================================================

@manager_bp.route('/products')
@login_required
def products():
    all_products = Product.query.all()
    return render_template('manager_app/products.html', products=all_products)


@manager_bp.route('/products/add', methods=['GET', 'POST'])
@login_required
def add_product():
    if request.method == 'POST':
        product = Product(
            name=request.form.get('name'),
            category=request.form.get('category'),
            price=float(request.form.get('price')),
            description=request.form.get('description'),
            stock_code=request.form.get('stock_code'),
            image_url=request.form.get('image_url', ''),
        )
        db.session.add(product)
        db.session.commit()
        flash('Product added successfully!', 'success')
        return redirect(url_for('manager_app.products'))

    return render_template('manager_app/product_form.html')


@manager_bp.route('/products/<int:product_id>/edit', methods=['GET', 'POST'])
@login_required
def edit_product(product_id):
    product = Product.query.get_or_404(product_id)

    if request.method == 'POST':
        product.name = request.form.get('name')
        product.category = request.form.get('category')
        product.price = float(request.form.get('price'))
        product.description = request.form.get('description')
        product.stock_code = request.form.get('stock_code')
        product.image_url = request.form.get('image_url', product.image_url)
        db.session.commit()
        flash('Product updated successfully!', 'success')
        return redirect(url_for('manager_app.products'))

    return render_template('manager_app/product_form.html', product=product)


@manager_bp.route('/products/<int:product_id>/delete', methods=['POST'])
@login_required
def delete_product(product_id):
    product = Product.query.get_or_404(product_id)
    db.session.delete(product)
    db.session.commit()
    flash('Product deleted successfully!', 'success')
    return redirect(url_for('manager_app.products'))


# ======================================================================
# Recommendations view
# ======================================================================

@manager_bp.route('/recommendations')
@login_required
def recommendations():
    all_recs = Recommendation.query.all()
    return render_template('manager_app/recommendations.html', recommendations=all_recs)
