from flask import Flask
from flask.templating import render_template
from flask import jsonify, redirect
from asgiref.wsgi import WsgiToAsgi

# Create a simple Flask app
app = Flask(__name__)

@app.route("/")
def index():
    return jsonify({"message": "ANTINORI Financial Portfolio Reporting API - Flask Version"})

@app.route("/health")
def health():
    return jsonify({"status": "healthy"})

# Create an ASGI app from the Flask app for compatibility
asgi_app = WsgiToAsgi(app)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)