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
        doc = request.form.get('doc')
        source = request.form.get('source')
        doc_id = request.form.get('doc_id')
        filetype = request.form.get('filetype')
    else:
        doc = request.args.get('doc')
        source = request.args.get('source')
        doc_id = request.args.get('doc_id')
        filetype = request.args.get('filetype')
    timestamp = process_docs.process_raw(doc, source, doc_id, filetype)


    with open( 'recent_files.txt', 'a') as f:
        f.write( source + ", " + doc_id + ", " + str(timestamp) + "\n" )

    return Response()


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

# find the source, docID ans timestamp that 's being used
# write those to a file with all the master documents (will be rewritten)
# in schedule.py define a task to run periodically:
# - read in the file, find all the raw files in there, 
# - request a parse for each of those files