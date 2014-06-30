from flask import Flask, request
import json
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
sys.path.insert(1, '/home/fabian/cos/scrapi/')
from api import process_docs

app = Flask(__name__)


@app.route('/process_raw', methods=['GET'])
def process_raw():
    doc = request.args.get('doc')
    source = request.args.get('source')
    doc_id = request.args.get('doc_id')
    filetype = request.args.get('filetype')

    return process_docs.process_raw(doc, source, doc_id, filetype)


@app.route('/process', methods=['GET', 'POST'])
def process():
    doc = json.loads(request.args.get('doc'))
    timestamp = request.args.get('timestamp')
    return process_docs.process(doc, timestamp)


if __name__ == '__main__':
    app.run(
        host="0.0.0.0",
        port=1337,
        debug=True
    )
