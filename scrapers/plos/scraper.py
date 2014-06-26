__author__ = 'faye'
import requests
import xmltodict
import time
from datetime import date, timedelta
import json
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

def parse_response(doc):
    print json.dumps(doc, sort_keys=True, indent=4)
    for result in doc["response"]["result"]["doc"]:
        if result["arr"][1]["@name"] == "abstract":


def retrieve():
    today = str(date.today())+"T00:00:00Z"
    yesterday = str(date.today() - timedelta(1))+"T00:00:00Z"
    payload = {"api_key": "7AbMQ-D-3hKy5hJ1FPY2", "rows": "0"}
    url = 'http://api.plos.org/search?q=publication_date:[{}%20TO%20{}]'.format(yesterday, today)
    plos_request = requests.get(url, params=payload)
    response = xmltodict.parse(plos_request.text)
    num_article = response["response"]["result"]["@numFound"]

    start = 0
    rows = 5

    while rows < 20:
        payload = {"api_key": "7AbMQ-D-3hKy5hJ1FPY2", "rows": rows, "start": start}
        results = requests.get(url, params=payload)
        tick = time.time()
        parse_response(xmltodict.parse(results.text))

        num_article = response["response"]["result"]["@numFound"]
        start += rows
        rows += rows
        if time.time()-tick < 5:
            time.sleep(5-(time.time()-tick))


    #process_docs.process_raw(text, 'basic_scraper', '10.1371%2Fjournal.pbio.1001356', 'xml')
    #parse(text)

retrieve()