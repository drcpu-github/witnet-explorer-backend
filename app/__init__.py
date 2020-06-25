from flask import Flask

from app.api import api

from app.cache import cache

app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False

cache.init_app(app=app)

app.register_blueprint(api, url_prefix="/api")
