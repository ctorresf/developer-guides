# Import necessary classes from the Flask library.
from flask import Flask, render_template, jsonify
import os
import requests

# Create the Flask application instance.
app = Flask(__name__)

# Define the route for the home page ('/').
@app.route('/')
def index():
    """
    This route renders the index.html template, serving the frontend to the user.
    """
    return render_template('index.html')

# Define an API endpoint that returns a JSON response.
@app.route('/api/data')
def get_data():
    backend_url = os.environ.get('BACKEND_HOST', 'localhost:8000')
    timestamp = "This data was fetched dynamically."

    try:
        response = requests.get(f'http://{backend_url}/api/message')
        response.raise_for_status()  # Raise an exception for bad status codes
        data = {
            "message": response.json()["message"],
            "timestamp": timestamp
        }
        return jsonify(data)
    except requests.exceptions.RequestException as e:
        data = {
            "message": str(e),
            "timestamp": timestamp
        }
        return jsonify(data)

# The entry point for the application.
if __name__ == '__main__':
    # Run the application.
    # The host='0.0.0.0' makes the app accessible externally,
    # and debug=True enables live reloading for development.
    app.run(host='0.0.0.0', debug=True, port=8080)

