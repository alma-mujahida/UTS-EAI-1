from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
import uuid
import os
import pika
import json
import requests
import time
from datetime import datetime

app = Flask(__name__)
CORS(app)

app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URL', 'postgresql://user:password@order-db:5432/orderdb')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
    'pool_recycle': 280,
    'pool_pre_ping': True,
}

RABBITMQ_URL = os.getenv('RABBITMQ_URL', 'amqp://guest:guest@rabbitmq:5672/')
USER_SERVICE_URL = os.getenv('USER_SERVICE_URL', 'http://user-service:5001')
PRODUCT_SERVICE_URL = os.getenv('PRODUCT_SERVICE_URL', 'http://product-service:5002')

db = SQLAlchemy(app)

# RabbitMQ connection management
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
            print(f"[✓] Order database tables created successfully")
            return
        except Exception as e:
            print(f"[!] Order DB init attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
            else:
                print(f"[!] Failed to initialize order database after {max_retries} attempts")

class Order(db.Model):
    __tablename__ = 'orders'
    id = db.Column(db.String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = db.Column(db.String(36), nullable=False)
    product_id = db.Column(db.String(36), nullable=False)
    quantity = db.Column(db.Integer, nullable=False)
    unit_price = db.Column(db.Numeric(12, 2), nullable=False)
    total_price = db.Column(db.Numeric(12, 2), nullable=False)
    status = db.Column(db.String(30), default='pending')  # pending, confirmed, paid, shipped, cancelled
    shipping_address = db.Column(db.Text)
    notes = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self):
        return {
            'id': self.id,
            'user_id': self.user_id,
            'product_id': self.product_id,
            'quantity': self.quantity,
            'unit_price': float(self.unit_price),
            'total_price': float(self.total_price),
            'status': self.status,
            'shipping_address': self.shipping_address,
            'notes': self.notes,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }

init_db()

def publish_message(queue_name, message):
    """Publish async message to RabbitMQ with retry logic"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            conn = get_rabbitmq_connection()
            channel = conn.channel()
            channel.queue_declare(queue=queue_name, durable=True)
            channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=json.dumps(message),
                properties=pika.BasicProperties(delivery_mode=2)  # persistent
            )
            print(f"[✓] Message published to {queue_name}")
            return True
        except Exception as e:
            print(f"[!] RabbitMQ publish attempt {attempt + 1}/{max_retries} failed: {e}")
            close_rabbitmq_connection()
            if attempt < max_retries - 1:
                time.sleep(2)
            else:
                print(f"[!] Failed to publish message after {max_retries} attempts")
                return False

# ── Routes ────────────────────────────────────────────────────────────────────

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'healthy', 'service': 'order-service'})

@app.route('/orders', methods=['GET'])
def list_orders():
    try:
        user_id = request.args.get('user_id')
        query = Order.query
        if user_id:
            query = query.filter_by(user_id=user_id)
        orders = query.order_by(Order.created_at.desc()).all()
        return jsonify({'success': True, 'data': [o.to_dict() for o in orders], 'total': len(orders)})
    except Exception as e:
        print(f"[!] Error listing orders: {e}")
        return jsonify({'success': False, 'message': 'Failed to load orders'}), 500

@app.route('/orders', methods=['POST'])
def create_order():
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'message': 'Invalid JSON data'}), 400
        
        required = ['user_id', 'product_id', 'quantity']
        for field in required:
            if field not in data:
                return jsonify({'success': False, 'message': f'{field} is required'}), 400

        # Validate user exists
        try:
            user_resp = requests.get(f"{USER_SERVICE_URL}/users/{data['user_id']}", timeout=5)
            if user_resp.status_code == 404:
                return jsonify({'success': False, 'message': 'User not found'}), 404
            if user_resp.status_code != 200:
                return jsonify({'success': False, 'message': f'User service error: HTTP {user_resp.status_code}'}), 503
            user_data = user_resp.json()
            if not user_data.get('success'):
                return jsonify({'success': False, 'message': 'User not found'}), 404
            user_data = user_data['data']
        except requests.RequestException as e:
            return jsonify({'success': False, 'message': f'User service unavailable: {str(e)}'}), 503

        # Validate product exists and get price
        try:
            prod_resp = requests.get(f"{PRODUCT_SERVICE_URL}/products/{data['product_id']}", timeout=5)
            if prod_resp.status_code == 404:
                return jsonify({'success': False, 'message': 'Product not found'}), 404
            if prod_resp.status_code != 200:
                return jsonify({'success': False, 'message': f'Product service error: HTTP {prod_resp.status_code}'}), 503
            product_data = prod_resp.json()
            if not product_data.get('success'):
                return jsonify({'success': False, 'message': 'Product not found'}), 404
            product_data = product_data['data']
        except requests.RequestException as e:
            return jsonify({'success': False, 'message': f'Product service unavailable: {str(e)}'}), 503

        if product_data['stock'] < data['quantity']:
            return jsonify({'success': False, 'message': 'Insufficient stock'}), 400

        unit_price = product_data['price']
        total_price = unit_price * data['quantity']

        order = Order(
            id=str(uuid.uuid4()),
            user_id=data['user_id'],
            product_id=data['product_id'],
            quantity=data['quantity'],
            unit_price=unit_price,
            total_price=total_price,
            status='confirmed',
            shipping_address=data.get('shipping_address', user_data.get('address', '')),
            notes=data.get('notes', '')
        )
        db.session.add(order)
        db.session.commit()

        # ASYNC: Send message to reduce stock via RabbitMQ
        published = publish_message('reduce_stock', {
            'order_id': order.id,
            'product_id': data['product_id'],
            'quantity': data['quantity']
        })

        # ASYNC: Send message to payment service
        publish_message('payment_queue', {
            'order_id': order.id,
            'user_id': data['user_id'],
            'product_id': data['product_id'],
            'total_price': total_price,
            'product_name': product_data['name']
        })

        return jsonify({
            'success': True,
            'message': 'Order created. Stock update and payment processing queued.',
            'data': order.to_dict(),
            'async_stock_queued': published
        }), 201
    except Exception as e:
        print(f"[!] Error creating order: {e}")
        db.session.rollback()
        return jsonify({'success': False, 'message': 'Failed to create order'}), 500

@app.route('/orders/<order_id>', methods=['GET'])
def get_order(order_id):
    try:
        order = Order.query.get(order_id)
        if not order:
            return jsonify({'success': False, 'message': 'Order not found'}), 404
        return jsonify({'success': True, 'data': order.to_dict()})
    except Exception as e:
        print(f"[!] Error getting order: {e}")
        return jsonify({'success': False, 'message': 'Failed to get order'}), 500

@app.route('/orders/<order_id>/status', methods=['PUT'])
def update_order_status(order_id):
    try:
        order = Order.query.get(order_id)
        if not order:
            return jsonify({'success': False, 'message': 'Order not found'}), 404
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'message': 'Invalid JSON data'}), 400
        valid_statuses = ['pending', 'confirmed', 'paid', 'shipped', 'delivered', 'cancelled']
        if data.get('status') not in valid_statuses:
            return jsonify({'success': False, 'message': f'Invalid status. Use: {valid_statuses}'}), 400
        order.status = data['status']
        order.updated_at = datetime.utcnow()
        db.session.commit()
        return jsonify({'success': True, 'message': 'Order status updated', 'data': order.to_dict()})
    except Exception as e:
        print(f"[!] Error updating order status: {e}")
        db.session.rollback()
        return jsonify({'success': False, 'message': 'Failed to update order status'}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5003, debug=True)
