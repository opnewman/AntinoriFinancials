from asgiref.wsgi import AsgiToWsgi
from main import app

# Convert FastAPI ASGI app to WSGI
wsgi_app = AsgiToWsgi(app)