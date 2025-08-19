# --- Here is an example of a simple Flask web application in Python ---
# Import the Flask class from the flask library.
# This is the core of our web application.
from flask import Flask

# Create a new Flask web application instance.
# We pass __name__ to the constructor, which helps Flask find
# resources like templates and static files.
app = Flask(__name__)

# Define a route for the root URL ('/').
# The @app.route decorator tells Flask which URL should trigger our function.
@app.route('/')
def home():
    """
    This function handles requests to the root URL.
    It returns an HTML string that will be displayed in the user's browser.
    """
    return '<h1>Hello from your Flask Application from VSCode devcontainer!</h1>'

# Define another route for a specific page.
@app.route('/about')
def about():
    """
    This function handles requests to the '/about' URL.
    It returns a different HTML string.
    """
    return '<h1>This is the About page.</h1><p>You have successfully navigated to a different route!</p>'

# This is the entry point for the application.
# It checks if the script is being run directly.
# If so, it starts the development server.
if __name__ == '__main__':
    # Run the application.
    # debug=True allows for auto-reloading the server when you make changes.
    # host='0.0.0.0' makes the server accessible from outside the container, which is
    # necessary when running in a Docker container.
    app.run(debug=True, host='0.0.0.0', port=8000)