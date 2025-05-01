import logging
import os
from flask import Flask, jsonify

# Set up logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Import database module
from src.database import init_db

# Initialize database (create tables)
init_db()

# Initialize Flask app
app = Flask(__name__)

# Root endpoint
@app.route("/")
def root():
    return jsonify({"message": "ANTINORI Financial Portfolio Reporting API"})

# Health check endpoint
@app.route("/health")
def health():
    return jsonify({"status": "healthy"})

# Run the server
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
