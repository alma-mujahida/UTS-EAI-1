from flask import Flask, request, jsonify
import json
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
from sqlalchemy.exc import IntegrityError
import uuid
import hashlib
import os
import time
from datetime import datetime

app = Flask(__name__)
CORS(app)

app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URL', 'postgresql://user:password@user-db:5432/userdb')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
    'pool_recycle': 280,
    'pool_pre_ping': True,
}

db = SQLAlchemy(app)

# Create tables on startup with retry logic
def init_db():
    max_retries = 5
    for attempt in range(max_retries):
        try:
            with app.app_context():
                db.create_all()
            print(f"[✓] Database tables created successfully")
            return
        except Exception as e:
            print(f"[!] Database init attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
            else:
                print(f"[!] Failed to initialize database after {max_retries} attempts")

class User(db.Model):
    __tablename__ = 'users'
    id = db.Column(db.String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    name = db.Column(db.String(100), nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password = db.Column(db.String(256), nullable=False)
    phone = db.Column(db.String(20))
    address = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'email': self.email,
            'phone': self.phone,
            'address': self.address,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }

init_db()

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'healthy', 'service': 'user-service'})

@app.route('/users', methods=['GET'])
def list_users():
    try:
        users = User.query.all()
        return jsonify({
            'success': True,
            'data': [u.to_dict() for u in users],
            'total': len(users)
        })
    except Exception as e:
        print(f"[!] Error listing users: {e}")
        return jsonify({'success': False, 'message': 'Failed to load users'}), 500

@app.route('/users/register', methods=['POST'])
def register():
    try:
        data = request.get_json(force=True, silent=True)
        if not data and request.data:
            try:
                data = json.loads(request.data.decode('utf-8'))
            except Exception:
                data = None
        if not data:
            return jsonify({'success': False, 'message': 'Invalid JSON data'}), 400

        required = ['name', 'email', 'password']
        for field in required:
            if field not in data or not str(data[field]).strip():
                return jsonify({'success': False, 'message': f'{field} is required'}), 400

        if User.query.filter_by(email=data['email']).first():
            return jsonify({'success': False, 'message': 'Email already registered'}), 409

        user = User(
            id=str(uuid.uuid4()),
            name=data['name'],
            email=data['email'],
            password=hash_password(data['password']),
            phone=data.get('phone'),
            address=data.get('address')
        )
        db.session.add(user)
        db.session.commit()
        db.session.refresh(user)

        return jsonify({'success': True, 'message': 'User registered successfully', 'data': user.to_dict()}), 201
    except IntegrityError:
        db.session.rollback()
        return jsonify({'success': False, 'message': 'Email already registered'}), 409
    except Exception as e:
        app.logger.exception("Error registering user")
        db.session.rollback()
        return jsonify({'success': False, 'message': f'Failed to register user: {str(e)}'}), 500

@app.route('/users/<user_id>', methods=['GET'])
def get_user(user_id):
    try:
        user = User.query.get(user_id)
        if not user:
            return jsonify({'success': False, 'message': 'User not found'}), 404
        return jsonify({'success': True, 'data': user.to_dict()})
    except Exception as e:
        print(f"[!] Error getting user: {e}")
        return jsonify({'success': False, 'message': 'Failed to get user'}), 500

@app.route('/users/<user_id>', methods=['PUT'])
def update_user(user_id):
    try:
        user = User.query.get(user_id)
        if not user:
            return jsonify({'success': False, 'message': 'User not found'}), 404

        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'message': 'Invalid JSON data'}), 400
        
        if 'name' in data: user.name = data['name']
        if 'phone' in data: user.phone = data['phone']
        if 'address' in data: user.address = data['address']
        db.session.commit()

        return jsonify({'success': True, 'message': 'User updated', 'data': user.to_dict()})
    except Exception as e:
        print(f"[!] Error updating user: {e}")
        db.session.rollback()
        return jsonify({'success': False, 'message': 'Failed to update user'}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)