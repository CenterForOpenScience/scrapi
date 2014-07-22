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



def normalize(result, timestamp):
    """Takes a clinicaltrials.gov nct_id and returns json in a format containing info about the trial in a format
    that can be imported by scrAPI."""
  
    result = xmltodict.parse(result)

    payload = {
        "doc": {
            'title': result["title"],
            'contributors': result["contributors"],
            'properties': {
                'abstract': result["description"]
            },
            'meta': {},
            'id': result["id"],
            'source': "ClinicalTrials.gov",
            'timestamp': str(timestamp)
        }
    }

        
    return payload