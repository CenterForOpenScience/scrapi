from flask import Flask, request, Response
import json
import sys
import os
sys.path.insert(1, os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    os.pardir,
))

from api import process_docs
import search

app = Flask(__name__)


@app.route('/process_raw', methods=['GET', 'POST'])
def process_raw():
    if request.method == 'POST':
        docs = request.form['doc']
        doc_list_item = docs.split("ASDFJKL")
        doc_ids = request.form['doc_id']
        doc_ids_item = doc_ids.split("ASDFJKL")
        for x in range(0,len(doc_list_item)):
            doc = doc_list_item[x]
            source = request.form.get('source')
            doc_id = doc_ids_item[x]
            filetype = request.form.get('filetype')

            Response(process_docs.process_raw(doc, source, doc_id, filetype))

    else:
        docs = request.args['doc']
        doc_list_item = docs.split("ASDFJKL")
        doc_ids = request.args['doc_id']
        doc_ids_item = doc_ids.split("ASDFJKL")
        for x in range(0,len(doc_list_item)):
            doc = doc_list_item[x]
            source = request.args.get('source')
            doc_id = doc_ids_item[x]
            filetype = request.args.get('filetype')

            Response(process_docs.process_raw(doc, source, doc_id, filetype))
    return "Processed!"


@app.route('/process', methods=['GET', 'POST'])
def process():
    if request.method == 'POST':
        doc = json.loads(request.form.get('doc'))
        timestamp = request.form.get('timestamp')
    else:
        doc = json.loads(request.args.get('doc'))
        timestamp = request.args.get('timestamp')
    processed_doc = process_docs.process(doc, timestamp)
    search.update('scrapi', doc, 'article', doc['id'])
    return processed_doc


@app.route('/search', methods=['GET'])
def search_search():
    query = request.args.get('q')
    start = request.args.get('from')
    size = request.args.get('size')
    return Response(json.dumps(search.search('scrapi', query, start, size), indent=4, sort_keys=True), mimetype='application/json')

if __name__ == '__main__':
    app.run(
        host="0.0.0.0",
        port=1337,
        debug=True
    )
