from flask import Flask
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # Allow CORS for all routes

@app.route('/')
def index():
    return "Hello from the Python backend!"
