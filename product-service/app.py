from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
import uuid
import os
import pika
import json
import threading
import time
from datetime import datetime

app = Flask(__name__)
CORS(app)

app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URL', 'postgresql://user:password@product-db:5432/productdb')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
    'pool_recycle': 280,
    'pool_pre_ping': True,
}
RABBITMQ_URL = os.getenv('RABBITMQ_URL', 'amqp://guest:guest@rabbitmq:5672/')

db = SQLAlchemy(app)
_consumer_thread = None

# RabbitMQ connection pool
_rabbitmq_connection = None

def get_rabbitmq_connection():
    """Get or create a RabbitMQ connection with retry logic"""
    global _rabbitmq_connection
    max_retries = 3
    for attempt in range(max_retries):
        try:
            if _rabbitmq_connection and not _rabbitmq_connection.is_closed:
                return _rabbitmq_connection
            params = pika.URLParameters(RABBITMQ_URL)
            _rabbitmq_connection = pika.BlockingConnection(params)
            return _rabbitmq_connection
        except Exception as e:
            print(f"[!] RabbitMQ connection attempt {attempt + 1}/{max_retries}: {e}")
            _rabbitmq_connection = None
            if attempt < max_retries - 1:
                time.sleep(2)
            else:
                raise

def close_rabbitmq_connection():
    """Close the RabbitMQ connection if open"""
    global _rabbitmq_connection
    if _rabbitmq_connection and not _rabbitmq_connection.is_closed:
        try:
            _rabbitmq_connection.close()
        except:
            pass
        _rabbitmq_connection = None

# Create tables on startup with retry logic
def init_db():
    max_retries = 5
    for attempt in range(max_retries):
        try:
            with app.app_context():
                db.create_all()
            print(f"[✓] Product database tables created successfully")
            return
        except Exception as e:
            print(f"[!] Product DB init attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
            else:
                print(f"[!] Failed to initialize product database after {max_retries} attempts")

class Product(db.Model):
    __tablename__ = 'products'
    id = db.Column(db.String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    name = db.Column(db.String(200), nullable=False)
    brand = db.Column(db.String(100), nullable=False)
    description = db.Column(db.Text)
    price = db.Column(db.Numeric(12, 2), nullable=False)
    stock = db.Column(db.Integer, default=0)
    size = db.Column(db.String(20))
    category = db.Column(db.String(50))
    image_url = db.Column(db.String(500))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'brand': self.brand,
            'description': self.description,
            'price': float(self.price),
            'stock': self.stock,
            'size': self.size,
            'category': self.category,
            'image_url': self.image_url,
            'created_at': self.created_at.isoformat()
        }

init_db()

def reduce_stock_consumer():
    """Async consumer: mengurangi stock ketika order dibuat"""
    import time
    while True:
        try:
            conn = get_rabbitmq_connection()
            channel = conn.channel()
            channel.queue_declare(queue='reduce_stock', durable=True)

            def callback(ch, method, properties, body):
                with app.app_context():
                    try:
                        data = json.loads(body)
                        product_id = data.get('product_id')
                        quantity = data.get('quantity', 1)

                        product = Product.query.get(product_id)
                        if product and product.stock >= quantity:
                            product.stock -= quantity
                            db.session.commit()
                            print(f"[✓] Stock reduced: {product.name} by {quantity}. Remaining: {product.stock}")
                            ch.basic_ack(delivery_tag=method.delivery_tag)
                        else:
                            print(f"[✗] Stock insufficient or product not found: {product_id}")
                            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    except Exception as e:
                        print(f"[!] Error processing stock: {e}")
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue='reduce_stock', on_message_callback=callback)
            print("[*] Product service listening for stock reduction messages...")
            channel.start_consuming()
        except Exception as e:
            print(f"[!] RabbitMQ connection error: {e}. Retrying in 5s...")
            time.sleep(5)

def start_reduce_stock_consumer_once():
    """Start RabbitMQ stock consumer once (works when app is loaded by gunicorn)."""
    global _consumer_thread
    if _consumer_thread and _consumer_thread.is_alive():
        return
    _consumer_thread = threading.Thread(target=reduce_stock_consumer, daemon=True)
    _consumer_thread.start()
    print("[✓] Product stock consumer thread started")

# ── Routes ────────────────────────────────────────────────────────────────────

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'healthy', 'service': 'product-service'})

@app.route('/products', methods=['GET'])
def list_products():
    try:
        category = request.args.get('category')
        brand = request.args.get('brand')
        query = Product.query
        if category:
            query = query.filter_by(category=category)
        if brand:
            query = query.filter_by(brand=brand)
        products = query.all()
        return jsonify({'success': True, 'data': [p.to_dict() for p in products], 'total': len(products)})
    except Exception as e:
        print(f"[!] Error listing products: {e}")
        return jsonify({'success': False, 'message': 'Failed to load products'}), 500

@app.route('/products', methods=['POST'])
def add_product():
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'message': 'Invalid JSON data'}), 400
        
        required = ['name', 'brand', 'price', 'stock']
        for field in required:
            if field not in data:
                return jsonify({'success': False, 'message': f'{field} is required'}), 400

        product = Product(
            id=str(uuid.uuid4()),
            name=data['name'],
            brand=data['brand'],
            description=data.get('description', ''),
            price=data['price'],
            stock=data['stock'],
            size=data.get('size'),
            category=data.get('category', 'sneakers'),
            image_url=data.get('image_url')
        )
        db.session.add(product)
        db.session.commit()
        return jsonify({'success': True, 'message': 'Product added', 'data': product.to_dict()}), 201
    except Exception as e:
        print(f"[!] Error adding product: {e}")
        db.session.rollback()
        return jsonify({'success': False, 'message': 'Failed to add product'}), 500

@app.route('/products/<product_id>', methods=['GET'])
def get_product(product_id):
    try:
        product = Product.query.get(product_id)
        if not product:
            return jsonify({'success': False, 'message': 'Product not found'}), 404
        return jsonify({'success': True, 'data': product.to_dict()})
    except Exception as e:
        print(f"[!] Error getting product: {e}")
        return jsonify({'success': False, 'message': 'Failed to get product'}), 500

@app.route('/products/<product_id>', methods=['DELETE'])
def delete_product(product_id):
    try:
        product = Product.query.get(product_id)
        if not product:
            return jsonify({'success': False, 'message': 'Product not found'}), 404
        db.session.delete(product)
        db.session.commit()
        return jsonify({'success': True, 'message': 'Product deleted'})
    except Exception as e:
        print(f"[!] Error deleting product: {e}")
        db.session.rollback()
        return jsonify({'success': False, 'message': 'Failed to delete product'}), 500

@app.route('/products/<product_id>/reduce-stock', methods=['POST'])
def reduce_stock_direct(product_id):
    """Direct stock reduction (called synchronously by order service)"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'message': 'Invalid JSON data'}), 400
        
        quantity = data.get('quantity', 1)
        product = Product.query.get(product_id)
        if not product:
            return jsonify({'success': False, 'message': 'Product not found'}), 404
        if product.stock < quantity:
            return jsonify({'success': False, 'message': 'Insufficient stock'}), 400
        product.stock -= quantity
        db.session.commit()
        return jsonify({'success': True, 'message': 'Stock reduced', 'remaining_stock': product.stock})
    except Exception as e:
        print(f"[!] Error reducing stock: {e}")
        db.session.rollback()
        return jsonify({'success': False, 'message': 'Failed to reduce stock'}), 500

# Ensure consumer starts when imported by gunicorn
start_reduce_stock_consumer_once()

if __name__ == '__main__':
    start_reduce_stock_consumer_once()
    app.run(host='0.0.0.0', port=5002, debug=True)