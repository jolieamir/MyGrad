from flask import Blueprint, render_template, request, session, redirect, url_for, flash
from app import db
from app.models import Product, Order, OrderItem, Recommendation, MiningResult
from datetime import datetime
import uuid

customer_bp = Blueprint('customer_app', __name__)

# Price cache (refreshes every 5 minutes)
_price_cache = {}
_price_cache_time = None
_sql_available = False


def _refresh_price_cache():
    """Refresh the SQL Server price cache if stale (>5 min)."""
    global _price_cache, _price_cache_time, _sql_available
    from datetime import datetime as dt

    now = dt.now()
    if _price_cache_time and (now - _price_cache_time).total_seconds() < 300:
        return  # Cache is fresh

    try:
        from data_mining.price_service import PriceService
        svc = PriceService()
        _price_cache = svc.get_all_product_prices()
        _sql_available = svc.is_connected()
        svc.close()
        _price_cache_time = now
    except Exception:
        _sql_available = False


def get_price(product):
    """Get product price: SQL Server cache first, fallback to product.price."""
    _refresh_price_cache()
    return _price_cache.get(product.name, product.price)


def get_cart():
    return session.get('cart', {})


def get_cart_total():
    total = 0
    for pid, qty in get_cart().items():
        product = Product.query.get(pid)
        if product:
            total += get_price(product) * qty
    return total


def get_cart_count():
    return sum(get_cart().values())


@customer_bp.context_processor
def inject_cart():
    return dict(cart_count=get_cart_count(), cart_total=get_cart_total)


# ======================================================================
# Pages
# ======================================================================

@customer_bp.route('/')
@customer_bp.route('/index')
def index():
    products = Product.query.all()
    products_with_prices = [
        {'product': p, 'price': get_price(p)} for p in products
    ]
    return render_template('customer_app/index.html',
                           products_with_prices=products_with_prices,
                           sql_server_available=_sql_available)


@customer_bp.route('/product/<int:product_id>')
def product_detail(product_id):
    product = Product.query.get_or_404(product_id)
    product_price = get_price(product)

    # Build recommendations per algorithm
    def _get_deduped_recs(pid, algo, limit=4):
        recs = Recommendation.query.filter_by(
            product_id=pid, algorithm=algo
        ).order_by(Recommendation.lift.desc()).all()
        seen = set()
        result = []
        for r in recs:
            if r.recommended_with_id not in seen and len(result) < limit:
                seen.add(r.recommended_with_id)
                result.append({
                    'product': r.recommended_with,
                    'price': get_price(r.recommended_with),
                    'confidence': r.confidence,
                    'lift': r.lift,
                })
        return result, seen

    def _get_also_like(pid, algo, exclude_ids, limit=4):
        """2nd-degree: recs of recs from the same algorithm."""
        similar = []
        used = set(exclude_ids)
        for rec_pid in list(exclude_ids):
            related = Recommendation.query.filter_by(
                product_id=rec_pid, algorithm=algo
            ).order_by(Recommendation.support.desc()).limit(6).all()
            for r in related:
                rid = r.recommended_with_id
                if rid != pid and rid not in used and len(similar) < limit:
                    used.add(rid)
                    similar.append({
                        'product': r.recommended_with,
                        'price': get_price(r.recommended_with),
                    })
        return similar

    # Get which algorithms have results
    algo_sections = []
    for algo in ['apriori', 'fpgrowth']:
        recs, seen_ids = _get_deduped_recs(product_id, algo, limit=4)
        if recs:
            also_like = _get_also_like(product_id, algo, seen_ids, limit=4)
            algo_sections.append({
                'name': algo.upper(),
                'label': 'FP-Growth' if algo == 'fpgrowth' else 'Apriori (SON)',
                'recs': recs,
                'also_like': also_like,
            })

    return render_template('customer_app/product.html',
                           product=product,
                           product_price=product_price,
                           algo_sections=algo_sections,
                           sql_server_available=_sql_available)


@customer_bp.route('/cart')
def cart():
    cart_data = get_cart()
    cart_items = []

    for pid, qty in cart_data.items():
        product = Product.query.get(pid)
        if product:
            price = get_price(product)
            cart_items.append({
                'product': product,
                'quantity': qty,
                'price': price,
                'subtotal': price * qty,
            })

    # Cross-sell recommendations based on cart items
    cart_product_ids = set(cart_data.keys())
    cross_sell = []
    if cart_product_ids:
        # Gather recommendation targets from all cart items
        rec_ids = set()
        for pid in cart_product_ids:
            recs = Recommendation.query.filter_by(product_id=int(pid)).order_by(
                Recommendation.lift.desc()
            ).limit(4).all()
            for r in recs:
                if str(r.recommended_with_id) not in cart_product_ids:
                    rec_ids.add(r.recommended_with_id)

        for rid in list(rec_ids)[:4]:
            p = Product.query.get(rid)
            if p:
                cross_sell.append({'product': p, 'price': get_price(p)})

        # Fallback if not enough recs
        if len(cross_sell) < 4:
            more = Product.query.filter(
                Product.id.notin_([int(x) for x in cart_product_ids])
            ).limit(4 - len(cross_sell)).all()
            cross_sell.extend([{'product': p, 'price': get_price(p)} for p in more])

    total = sum(item['subtotal'] for item in cart_items)

    return render_template('customer_app/cart.html',
                           cart_items=cart_items,
                           total=total,
                           cross_sell_products=cross_sell,
                           sql_server_available=_sql_available)


@customer_bp.route('/cart/add/<int:product_id>', methods=['POST'])
def add_to_cart(product_id):
    Product.query.get_or_404(product_id)

    if 'cart' not in session:
        session['cart'] = {}

    cart = session['cart']
    pid = str(product_id)
    cart[pid] = cart.get(pid, 0) + 1
    session['cart'] = cart
    session.modified = True

    flash('Added to cart!', 'success')
    return redirect(request.referrer or url_for('customer_app.index'))


@customer_bp.route('/cart/remove/<int:product_id>', methods=['POST'])
def remove_from_cart(product_id):
    if 'cart' in session:
        cart = session['cart']
        pid = str(product_id)
        if pid in cart:
            del cart[pid]
            session['cart'] = cart
            session.modified = True
            flash('Removed from cart', 'info')
    return redirect(url_for('customer_app.cart'))


@customer_bp.route('/cart/update/<int:product_id>', methods=['POST'])
def update_cart(product_id):
    quantity = int(request.form.get('quantity', 1))
    if 'cart' in session:
        cart = session['cart']
        pid = str(product_id)
        if pid in cart:
            if quantity <= 0:
                del cart[pid]
            else:
                cart[pid] = quantity
            session['cart'] = cart
            session.modified = True
    return redirect(url_for('customer_app.cart'))


@customer_bp.route('/checkout', methods=['GET', 'POST'])
def checkout():
    cart_data = get_cart()
    if not cart_data:
        flash('Your cart is empty', 'warning')
        return redirect(url_for('customer_app.index'))

    if request.method == 'POST':
        customer_name = request.form.get('customer_name')
        customer_email = request.form.get('customer_email')

        if not customer_name:
            flash('Please enter your name', 'error')
            return render_template('customer_app/checkout.html')

        order = Order(
            order_number=f"ORD-{uuid.uuid4().hex[:8].upper()}",
            customer_name=customer_name,
            customer_email=customer_email,
            total_amount=get_cart_total(),
            status='completed',
        )
        db.session.add(order)
        db.session.commit()

        for pid, qty in cart_data.items():
            product = Product.query.get(pid)
            if product:
                price = get_price(product)
                db.session.add(OrderItem(
                    order_id=order.id,
                    product_id=product.id,
                    quantity=qty,
                    price=price,
                ))
        db.session.commit()

        session.pop('cart', None)
        flash(f'Order {order.order_number} placed!', 'success')
        return redirect(url_for('customer_app.order_confirmation', order_id=order.id))

    return render_template('customer_app/checkout.html')


@customer_bp.route('/order/<int:order_id>/confirmation')
def order_confirmation(order_id):
    order = Order.query.get_or_404(order_id)
    return render_template('customer_app/confirmation.html', order=order)


@customer_bp.route('/search')
def search():
    query = request.args.get('q', '')
    if query:
        products = Product.query.filter(
            (Product.name.ilike(f'%{query}%')) |
            (Product.description.ilike(f'%{query}%'))
        ).all()
    else:
        products = Product.query.all()

    products_with_prices = [
        {'product': p, 'price': get_price(p)} for p in products
    ]
    return render_template('customer_app/search.html',
                           products_with_prices=products_with_prices,
                           query=query,
                           sql_server_available=_sql_available)
