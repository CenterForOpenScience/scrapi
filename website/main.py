from flask import Flask, request, Response, render_template, send_file, abort
import json
import sys
import os
sys.path.insert(1, os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    os.pardir,
))

from api import process_docs
import search
import logging
import rss_feed


logger = logging.getLogger(__name__)
app = Flask(__name__)


@app.route('/', methods=['GET'])
def home():
    with open('static/html/index.html', 'r') as f:
        return f.read()


@app.route('/process_raw', methods=['GET', 'POST'])
def process_raw():
    if request.method == 'POST':
        docs = request.form['doc']
        doc_list_item = docs.split("ASDFJKL")  # TODO Fix this
        doc_ids = request.form['doc_id']
        doc_ids_item = doc_ids.split("ASDFJKL")
        for x in range(0, len(doc_list_item)):
            doc = doc_list_item[x]
            source = request.form.get('source')
            doc_id = doc_ids_item[x]
            filetype = request.form.get('filetype')

            timestamp = process_docs.process_raw(doc, source, doc_id, filetype)
            with open('worker_manager/recent_files.txt', 'a') as f:
                f.write(source + ", " + doc_id + ", " + str(timestamp) + "\n")
    else:
        docs = request.args['doc']
        doc_list_item = docs.split("ASDFJKL")
        doc_ids = request.args['doc_id']
        doc_ids_item = doc_ids.split("ASDFJKL")
        for x in range(0, len(doc_list_item)):
            doc = doc_list_item[x]
            source = request.args.get('source')
            doc_id = doc_ids_item[x]
            filetype = request.args.get('filetype')

            timestamp = process_docs.process_raw(doc, source, doc_id, filetype)
            with open('worker_manager/recent_files.txt', 'a') as f:
                f.write(source + ", " + doc_id + ", " + str(timestamp) + "\n")

    return Response()


@app.route('/process', methods=['GET', 'POST'])
def process():
    if request.method == 'POST':
        doc = json.loads(request.form.get('doc'))
        timestamp = request.form.get('timestamp')
    else:
        doc = json.loads(request.args.get('doc'))
        timestamp = request.args.get('timestamp')
    doc['timestamp'] = timestamp
    processed_doc = process_docs.process(doc, timestamp)
    search.update('scrapi', doc, doc['source'], doc['id'])
    return processed_doc


@app.route('/api/search', methods=['GET'])
def search_search():
    query = request.args.get('q')
    start = request.args.get('from')
    size = request.args.get('size')
    # return render_template('search.html.jinja2', results=search.search('scrapi', query, start, size))
    results, count = search.search('scrapi', query, start, size)
    return json.dumps(results)


@app.route('/archive/', defaults={'req_path': ''})
@app.route('/archive/<path:req_path>')
def archive_exploration(req_path):
    BASE_DIR = '../archive'
    abs_path = os.path.join(BASE_DIR, req_path)

    if not os.path.exists(abs_path):
        return abort(404)

    if os.path.isfile(abs_path):
        return send_file(abs_path)

    files = os.listdir(abs_path)
    BASE_URL = '/archive' if not req_path else '/archive/' + req_path

    return render_template('files.html', files=files, url=BASE_URL)


@app.route('/rss/', defaults={'req_path': ''})
@app.route('/rss/<path:req_path>')
def rss(req_path):
    return rss_feed.gen_rss_feed(request.args.get("q"))


if __name__ == '__main__':
    app.run(
        host="0.0.0.0",
        port=1337,
        debug=False
    )
