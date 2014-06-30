__author__ = 'faye'
import requests
import json
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


def parse(result, dirname):
    timestamp = dirname.split('/')[-1]

    result = json.loads(result)
    payload = {
        "doc":
            json.dumps({
                'title': result["str"][4]["#text"],
                'contributors': result["arr"][0]["str"],
                'properties': {
                    'description': result["arr"][1]["str"],
                },
                'meta': {},
                'id': result["str"][0]["#text"],
                'source': "PLoS"
            }),
        'timestamp': timestamp
    }
    requests.get('http://0.0.0.0:1337/process', params=payload)
