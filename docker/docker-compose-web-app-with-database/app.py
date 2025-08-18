import os
from flask import Flask, jsonify, request, render_template
from flask_sqlalchemy import SQLAlchemy

# Create the Flask application instance.
app = Flask(__name__)

# Configure the database connection from environment variables.
# This makes the app flexible for different environments.
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Initialize the SQLAlchemy object with the Flask app.
db = SQLAlchemy(app)

# Define the User model. This maps to the `users` table in the database.
class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)

    def __repr__(self):
        return f'<User {self.username}>'

# Create the database and the table on application start.
# This is a simple way to handle migrations for this example.
with app.app_context():
    db.create_all()

# --- New Root Route for the UI ---
@app.route('/')
def index():
    """Renders the HTML template for the user interface."""
    return render_template('index.html')

# API endpoint to add a new user.
@app.route('/users', methods=['POST'])
def add_user():
    """Adds a new user to the database."""
    try:
        data = request.get_json()
        if not data or 'username' not in data or 'email' not in data:
            return jsonify({'error': 'Missing username or email'}), 400

        new_user = User(username=data['username'], email=data['email'])
        db.session.add(new_user)
        db.session.commit()
        return jsonify({'message': 'User added successfully!', 'user_id': new_user.id}), 201
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500

# API endpoint to get all users.
@app.route('/users', methods=['GET'])
def get_users():
    """Retrieves all users from the database."""
    users = User.query.all()
    user_list = [{'id': user.id, 'username': user.username, 'email': user.email} for user in users]
    return jsonify(user_list)

# Main entry point for the application.
if __name__ == '__main__':
    # The host '0.0.0.0' makes the app accessible from outside the container.
    app.run(host='0.0.0.0', port=8080, debug=True)
