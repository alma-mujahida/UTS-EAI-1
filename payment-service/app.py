from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
import uuid
import os
import pika
import json
import threading
import requests
import time
from datetime import datetime

app = Flask(__name__)
CORS(app)

app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URL', 'postgresql://user:password@payment-db:5432/paymentdb')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
    'pool_recycle': 280,
    'pool_pre_ping': True,
}

RABBITMQ_URL = os.getenv('RABBITMQ_URL', 'amqp://guest:guest@rabbitmq:5672/')
ORDER_SERVICE_URL = os.getenv('ORDER_SERVICE_URL', 'http://order-service:5003')

db = SQLAlchemy(app)
_consumer_thread = None

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
            print(f"[✓] Payment database tables created successfully")
            return
        except Exception as e:
            print(f"[!] Payment DB init attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
            else:
                print(f"[!] Failed to initialize payment database after {max_retries} attempts")

class Payment(db.Model):
    __tablename__ = 'payments'
    id = db.Column(db.String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    order_id = db.Column(db.String(36), unique=True, nullable=False)
    user_id = db.Column(db.String(36), nullable=False)
    product_id = db.Column(db.String(36), nullable=False)
    amount = db.Column(db.Numeric(12, 2), nullable=False)
    method = db.Column(db.String(50), default='bank_transfer')  # bank_transfer, gopay, ovo, dana, qris
    status = db.Column(db.String(30), default='pending')  # pending, processing, success, failed, refunded
    transaction_ref = db.Column(db.String(100), unique=True)
    payment_proof = db.Column(db.String(500))  # URL bukti bayar
    product_name = db.Column(db.String(200))
    paid_at = db.Column(db.DateTime)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self):
        return {
            'id': self.id,
            'order_id': self.order_id,
            'user_id': self.user_id,
            'product_id': self.product_id,
            'amount': float(self.amount),
            'method': self.method,
            'status': self.status,
            'transaction_ref': self.transaction_ref,
            'product_name': self.product_name,
            'paid_at': self.paid_at.isoformat() if self.paid_at else None,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }

init_db()

def generate_transaction_ref():
    return f"TXN-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}-{str(uuid.uuid4())[:8].upper()}"

def notify_order_paid(order_id):
    """Update order status to paid after successful payment"""
    try:
        requests.put(f"{ORDER_SERVICE_URL}/orders/{order_id}/status",
                     json={'status': 'paid'}, timeout=5)
    except Exception as e:
        print(f"[!] Failed to notify order service: {e}")

def payment_consumer():
    """Async: Auto-create payment record when order is created"""
    while True:
        try:
            conn = get_rabbitmq_connection()
            channel = conn.channel()
            channel.queue_declare(queue='payment_queue', durable=True)

            def callback(ch, method, properties, body):
                with app.app_context():
                    try:
                        data = json.loads(body)
                        existing = Payment.query.filter_by(order_id=data['order_id']).first()
                        if not existing:
                            payment = Payment(
                                id=str(uuid.uuid4()),
                                order_id=data['order_id'],
                                user_id=data['user_id'],
                                product_id=data['product_id'],
                                amount=data['total_price'],
                                product_name=data.get('product_name', ''),
                                transaction_ref=generate_transaction_ref(),
                                status='pending'
                            )
                            db.session.add(payment)
                            db.session.commit()
                            print(f"[✓] Payment record created for order {data['order_id']}: Rp {data['total_price']:,.0f}")
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                    except Exception as e:
                        print(f"[!] Error creating payment: {e}")
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue='payment_queue', on_message_callback=callback)
            print("[*] Payment service listening for payment messages...")
            channel.start_consuming()
        except Exception as e:
            print(f"[!] RabbitMQ error: {e}. Retrying in 5s...")
            close_rabbitmq_connection()
            time.sleep(5)

def start_payment_consumer_once():
    """Start RabbitMQ consumer thread once (works for gunicorn too)."""
    global _consumer_thread
    if _consumer_thread and _consumer_thread.is_alive():
        return
    _consumer_thread = threading.Thread(target=payment_consumer, daemon=True)
    _consumer_thread.start()
    print("[✓] Payment consumer thread started")

# ── Routes ────────────────────────────────────────────────────────────────────

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'healthy', 'service': 'payment-service'})

@app.route('/payments', methods=['GET'])
def list_payments():
    try:
        user_id = request.args.get('user_id')
        query = Payment.query
        if user_id:
            query = query.filter_by(user_id=user_id)
        payments = query.order_by(Payment.created_at.desc()).all()
        return jsonify({'success': True, 'data': [p.to_dict() for p in payments], 'total': len(payments)})
    except Exception as e:
        print(f"[!] Error listing payments: {e}")
        return jsonify({'success': False, 'message': 'Failed to load payments'}), 500

@app.route('/payments/<payment_id>', methods=['GET'])
def get_payment(payment_id):
    try:
        payment = Payment.query.get(payment_id)
        if not payment:
            return jsonify({'success': False, 'message': 'Payment not found'}), 404
        return jsonify({'success': True, 'data': payment.to_dict()})
    except Exception as e:
        print(f"[!] Error getting payment: {e}")
        return jsonify({'success': False, 'message': 'Failed to get payment'}), 500

@app.route('/payments/by-order/<order_id>', methods=['GET'])
def get_payment_by_order(order_id):
    try:
        payment = Payment.query.filter_by(order_id=order_id).first()
        if not payment:
            return jsonify({'success': False, 'message': 'Payment not found for this order'}), 404
        return jsonify({'success': True, 'data': payment.to_dict()})
    except Exception as e:
        print(f"[!] Error getting payment by order: {e}")
        return jsonify({'success': False, 'message': 'Failed to get payment'}), 500

@app.route('/payments/<payment_id>/pay', methods=['POST'])
def process_payment(payment_id):
    """Process / confirm a payment"""
    try:
        payment = Payment.query.get(payment_id)
        if not payment:
            return jsonify({'success': False, 'message': 'Payment not found'}), 404
        if payment.status == 'success':
            return jsonify({'success': False, 'message': 'Payment already processed'}), 400

        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'message': 'Invalid JSON data'}), 400
        
        valid_methods = ['bank_transfer', 'gopay', 'ovo', 'dana', 'qris', 'credit_card']
        method = data.get('method', 'bank_transfer')
        if method not in valid_methods:
            return jsonify({'success': False, 'message': f'Invalid method. Choose: {valid_methods}'}), 400

        payment.method = method
        payment.status = 'success'
        payment.paid_at = datetime.utcnow()
        payment.payment_proof = data.get('payment_proof', '')
        payment.updated_at = datetime.utcnow()
        db.session.commit()

        # Notify order service async
        threading.Thread(target=notify_order_paid, args=(payment.order_id,), daemon=True).start()

        return jsonify({
            'success': True,
            'message': 'Payment confirmed!',
            'data': payment.to_dict()
        })
    except Exception as e:
        print(f"[!] Error processing payment: {e}")
        db.session.rollback()
        return jsonify({'success': False, 'message': 'Failed to process payment'}), 500

@app.route('/payments/<payment_id>/refund', methods=['POST'])
def refund_payment(payment_id):
    try:
        payment = Payment.query.get(payment_id)
        if not payment:
            return jsonify({'success': False, 'message': 'Payment not found'}), 404
        if payment.status != 'success':
            return jsonify({'success': False, 'message': 'Only successful payments can be refunded'}), 400

        payment.status = 'refunded'
        payment.updated_at = datetime.utcnow()
        db.session.commit()
        return jsonify({'success': True, 'message': 'Payment refunded', 'data': payment.to_dict()})
    except Exception as e:
        print(f"[!] Error refunding payment: {e}")
        db.session.rollback()
        return jsonify({'success': False, 'message': 'Failed to refund payment'}), 500

@app.route('/payments/summary', methods=['GET'])
def payment_summary():
    try:
        from sqlalchemy import func
        total_revenue = db.session.query(func.sum(Payment.amount)).filter_by(status='success').scalar() or 0
        total_orders = Payment.query.filter_by(status='success').count()
        pending = Payment.query.filter_by(status='pending').count()
        return jsonify({
            'success': True,
            'data': {
                'total_revenue': float(total_revenue),
                'successful_payments': total_orders,
                'pending_payments': pending
            }
        })
    except Exception as e:
        print(f"[!] Error getting payment summary: {e}")
        return jsonify({'success': False, 'message': 'Failed to get payment summary'}), 500

# Ensure consumer starts when app is imported by gunicorn
start_payment_consumer_once()

if __name__ == '__main__':
    start_payment_consumer_once()
    app.run(host='0.0.0.0', port=5004, debug=True)
