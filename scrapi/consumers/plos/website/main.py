import json
import sys

from flask import Flask, request, Response

import consume
import parse

reload(sys)
sys.setdefaultencoding('utf-8')
sys.path.insert(1, '/home/faye/cos/scrapi/')

app = Flask(__name__)
app.debug = True

@app.route('/consume', methods=['GET', 'POST'])
def consume_day():
    return Response(consume.consume())


@app.route('/process', methods=['GET', 'POST'])
def parse_all():
    result = request.args.get('doc')
    timestamp = request.args.get('timestamp')
    return Response(parse.parse(result, timestamp))


if __name__ == '__main__':
    app.run(
        host="0.0.0.0",
        port=1338,
        debug=True
    )
