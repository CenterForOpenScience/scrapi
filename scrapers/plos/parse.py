__author__ = 'faye'
import requests
import json
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
sys.path.insert(1, '/home/faye/cos/scrapi/')

def parse(result):
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
                'source': result["str"][1]["#text"]
            })
        }
    requests.get('http://0.0.0.0:1337/process', params=payload)
