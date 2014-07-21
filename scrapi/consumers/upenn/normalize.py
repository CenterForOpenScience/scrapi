import sys
import xmltodict
from bs4 import BeautifulSoup
reload(sys)
sys.setdefaultencoding('utf-8')


def normalize(result, timestamp):
    result = xmltodict.parse(result)
    payload = {
        "doc": {
            'title': result["str"][4]["#text"],
            'contributors': result["arr"][0]["str"],
            'properties': {
        'abstract': result["arr"][1]["str"]
            },
            'meta': {},
            'id': result["str"][0]["#text"],
            'source': "ClinicalTrials.gov",
            'timestamp': str(timestamp)
        }
    }
    return payload
