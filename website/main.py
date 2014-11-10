from flask import Flask, request, Response, render_template, send_file, abort, jsonify
import json
import sys
import os
sys.path.insert(1, os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    os.pardir,
))

import search
import process_metadata
import logging

from scrapi import settings

logger = logging.getLogger(__name__)
app = Flask(__name__)


@app.route('/', methods=['GET'])
def home():
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static/html/index.html'), 'r') as f:
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
    abs_path = os.path.join(os.path.abspath('.'), 'archive', req_path)
    if not os.path.exists(abs_path):
        return abort(404)

    if os.path.isfile(abs_path):
        return send_file(abs_path)

    files = os.listdir(abs_path)
    BASE_URL = '/archive' if not req_path else '/archive/' + req_path

    return render_template('files.html', files=files, url=BASE_URL)


@app.route('/api/v1/SHARE/', methods=['GET', 'POST'])
def process_incoming_metadata():
    if request.method == 'GET':
        return_value = jsonify(process_metadata.tutorial())
    elif request.get_data():
        auth = request.authorization

        if auth.password not in settings.SCRAPI_KEY:
            return abort(401)

        url_data = request.get_data()
        process = process_metadata.process_api_input(url_data)

        return_value = 'You gave us...\n {}'.format(url_data)

    return return_value

if __name__ == '__main__':
    app.run(
        host="0.0.0.0",
        port=1337,
        debug=settings.SCRAPI_SERVER_DEBUG
    )
