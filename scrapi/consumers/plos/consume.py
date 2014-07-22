__author__ = 'faye'
import requests
from bs4 import BeautifulSoup
import xmltodict
import dicttoxml
import time
import os
from datetime import date, timedelta
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from website import settings


def consume():
    doc_list = []
    today = str(date.today()) + "T00:00:00Z"
    yesterday = str(date.today() - timedelta(4)) + "T00:00:00Z"
    payload = {"api_key": settings.API_KEY, "rows": "0"}
    url = 'http://api.plos.org/search?q=publication_date:[{}%20TO%20{}]'.format(yesterday, today)
    plos_request = requests.get(url, params=payload)
    response = xmltodict.parse(plos_request.text)
    num_articles = int(response["response"]["result"]["@numFound"])

    start = 0
    rows = 10
    raw = ""

    while rows < 50:
        payload = {"api_key": settings.API_KEY, "rows": rows, "start": start}
        results = requests.get(url, params=payload)
        tick = time.time()

        raw += "\n" + results.text

        doc = xmltodict.parse(results.text)

        full_response = doc["response"]["result"]["doc"]

        # TODO Incooporate "Correction" article type
        try:
            for result in full_response:
                try:
                    if result["arr"][1]["@name"] == "abstract" and result["str"][3]["#text"] == "Research Article":
                        doc_list.append(result)
                except KeyError:
                    pass

            start += rows
            rows += rows

            if time.time() - tick < 5:
                time.sleep(5 - (time.time() - tick))
        except KeyError:
            print "No new files/updates!"
            break

    with open(os.path.abspath(os.path.join(os.path.abspath(__file__), os.pardir)) + '/version', 'r') as f:
        version = f.readline()

    return_list = []
    for x in range(0, len(doc_list)):
        doc_xml = doc_list[x]
        doc_id = doc_xml["str"][0]["#text"]
        return_list.append((dicttoxml.dicttoxml(doc_list[x]), 'PLoS', doc_id, 'xml', version))

#    payload = {
#        'doc': 'ASDFJKL'.join(doc_list),
#        'source': 'PLoS',
#        'doc_id': 'ASDFJKL'.join(full_id),
#        'filetype': 'html',
#        'version': version,
#    }

    # requests.post('http://0.0.0.0:1337/process_raw', data=payload)
    return return_list

print consume()