__author__ = 'faye'
import sys
import xmltodict
reload(sys)
sys.setdefaultencoding('utf-8')


def normalize(result, timestamp):
    result = xmltodict.parse(result)

    return {
        'title': result["str"][4]["#text"] if result.get("str") else None,
        'contributors': result["arr"][0]["str"] if result.get("arr") else None,
        'properties': {
            'abstract': result["arr"][1]["str"] if result.get("arr") else None,
        },
        'meta': {},
        'id': result["str"][0]["#text"] if result.get("str") else None,
        'source': "PLoS",
        'timestamp': str(timestamp)
    }
