from app import db, login_manager
from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime

# User model for admin authentication
class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(64), unique=True, nullable=False)
    password_hash = db.Column(db.String(256), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    def set_password(self, password):
        self.password_hash = generate_password_hash(password)
    
    def check_password(self, password):
        return check_password_hash(self.password_hash, password)
    
    @classmethod
    def create_admin(cls):
        """Create default admin user if doesn't exist"""
        admin = cls.query.filter_by(username='admin').first()
        if not admin:
            admin = cls(username='admin')
            admin.set_password('admin123')  # Change this in production
            db.session.add(admin)
            db.session.commit()
            return admin
        return admin

# Product model for catalog
class Product(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(200), nullable=False)
    category = db.Column(db.String(100))
    price = db.Column(db.Float, nullable=False)
    image_url = db.Column(db.String(500))
    description = db.Column(db.Text)
    stock_code = db.Column(db.String(50))
    
    def __repr__(self):
        return f'<Product {self.name}>'

# Recommendation model (from mining results)
class Recommendation(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    product_id = db.Column(db.Integer, db.ForeignKey('product.id'), nullable=False)
    recommended_with_id = db.Column(db.Integer, db.ForeignKey('product.id'), nullable=False)
    confidence = db.Column(db.Float)
    support = db.Column(db.Float)
    lift = db.Column(db.Float)
    algorithm = db.Column(db.String(50), default='fpgrowth')  # 'fpgrowth' or 'apriori'

    product = db.relationship('Product', foreign_keys=[product_id], backref='recommendations')
    recommended_with = db.relationship('Product', foreign_keys=[recommended_with_id])

# Mining results storage
class MiningResult(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    algorithm = db.Column(db.String(50), nullable=False)  # 'apriori' or 'fpgrowth'
    parameters = db.Column(db.Text)  # JSON string of parameters
    results = db.Column(db.Text)  # JSON string of results
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    created_by = db.Column(db.String(64))  # username
    
    def __repr__(self):
        return f'<MiningResult {self.algorithm} {self.created_at}>'

# Order model for customer purchases
class Order(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    order_number = db.Column(db.String(20), unique=True, nullable=False)
    customer_name = db.Column(db.String(100), nullable=False)
    customer_email = db.Column(db.String(120))
    total_amount = db.Column(db.Float, nullable=False)
    status = db.Column(db.String(20), default='completed')  # completed, cancelled
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    items = db.relationship('OrderItem', backref='order', lazy=True)
    
    def __repr__(self):
        return f'<Order {self.order_number}>'

# Order items
class OrderItem(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    order_id = db.Column(db.Integer, db.ForeignKey('order.id'), nullable=False)
    product_id = db.Column(db.Integer, db.ForeignKey('product.id'), nullable=False)
    quantity = db.Column(db.Integer, nullable=False, default=1)
    price = db.Column(db.Float, nullable=False)  # Price at time of purchase
    
    product = db.relationship('Product')
    
    def __repr__(self):
        return f'<OrderItem {self.product.name} x{self.quantity}>'

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))