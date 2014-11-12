import os
import sys
import json
import logging
import httplib as http

from flask import abort
from flask import Flask
from flask import jsonify
from flask import request
from flask import Response
from flask import send_file
from flask import render_template

from website import search

from website import process_metadata

from scrapi import settings

app = Flask(__name__, static_folder='website/static/', static_url_path='/static')
logger = logging.getLogger(__name__)


@app.route('/', methods=['GET'])
def home():
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'website/static/html/index.html'), 'r') as f:
        return f.read()


@app.route('/api/search', methods=['GET'])
def search_search():
    """
    q=None
    start_date=DATE
    end_date=DATE
    sort_field=FIELD
    sort_type={asc | desc}
    from=START
    size=SIZE
    format=OUTPUT FORMAT
    """
    if len(request.args.keys()) == 0:
        return jsonify(search.tutorial())

    return jsonify(search.search(request.args))


@app.route('/archive/', defaults={'req_path': ''})
@app.route('/archive/<path:req_path>')
def archive_exploration(req_path):
    abs_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), 'archive', req_path))
    if not os.path.exists(abs_path):
        return abort(http.NOT_FOUND)

    if os.path.isfile(abs_path):
        return send_file(abs_path)

    files = os.listdir(abs_path)
    BASE_URL = '/archive' if not req_path else '/archive/' + req_path

    return render_template('files.html', files=files, url=BASE_URL)


@app.route('/api/v1/share/', methods=['GET'])
def show_tutorial():
    return jsonify(process_metadata.TUTORIAL)


@app.route('/api/v1/share/', methods=['POST'])
def process_incoming_metadata():
    data = request.get_json()
    auth = request.authorization

    if auth.password not in settings.SCRAPI_KEY:
        return abort(http.UNAUTHORIZED)

    try:
        process = process_metadata.process_api_input(data)
    except TypeError as e:
        return jsonify({'message': e.message}), http.BAD_REQUEST

    return jsonify({}), http.CREATED


if __name__ == '__main__':
    app.run(
        host="0.0.0.0",
        port=1337,
        debug=settings.SCRAPI_SERVER_DEBUG
    )
