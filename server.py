import os
import json
import logging
import httplib as http

import requests

from flask import abort
from flask import Flask
from flask import jsonify
from flask import request
from flask import send_file
from flask import send_from_directory

from scrapi import settings

from website import search
from website import process_metadata

logger = logging.getLogger(__name__)
app = Flask(
    __name__,
    static_folder='website/static/',
    static_url_path='/static'
)

HEADERS = {'Content-Type': 'application/json'}


@app.route('/', methods=['GET'])
def home():
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           'website/static/html/index.html'), 'r') as f:
        return f.read()


@app.route('/robots.txt', methods=['GET'])
def robots():
    return send_from_directory(app.static_folder, 'robots.txt')


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
    if not request.args:
        return jsonify(search.tutorial())

    return jsonify(search.search(request.args))


@app.route('/archive/', defaults={'req_path': ''})
@app.route('/archive/<path:req_path>')
def archive_exploration(req_path):
    abs_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), 'archive', req_path))
    try:
        return send_file(abs_path)
    except IOError:
        return abort(http.NOT_FOUND)


@app.route('/api/v1/share/', methods=['GET'])
def show_tutorial():
    return jsonify(process_metadata.TUTORIAL)


@app.route('/api/v1/share/', methods=['POST'])
def process_incoming_metadata():
    data = request.get_json()
    auth = request.authorization or {}
    url = '{0}auth/'.format(settings.OSF_APP_URL)

    osf_auth = requests.post(url, auth=settings.OSF_AUTH, headers=HEADERS,
                             data=json.dumps({'key': auth.get('password')})).json()

    if 'write' not in osf_auth.get('permissions', []):
        return abort(http.UNAUTHORIZED)

    try:
        process_metadata.process_api_input(data['events'])
    except TypeError as e:
        return jsonify({'message': e.message}), http.BAD_REQUEST

    return jsonify({}), http.CREATED


if __name__ == '__main__':
    app.run(
        host='0.0.0.0',
        port=1337,
        debug=settings.DEBUG
    )
