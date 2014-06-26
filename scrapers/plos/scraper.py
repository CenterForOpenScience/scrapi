__author__ = 'faye'
import requests
import xmltodict
import time
import settings
from datetime import date, timedelta
import api.process_docs as process_docs
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
sys.path.insert(1, '/home/faye/cos/scrapi/')

def parse_response(doc):
    formatted_results = []
    for result in doc["response"]["result"]["doc"]:
        try:
            if result["arr"][1]["@name"] == "abstract":
                formatted_results.append(
                    {
                        'title': result["str"][4]["#text"],
                        'contributors': result["arr"][0]["str"],
                        'properties': {
                            'description': result["arr"][1]["str"],
                            },
                        'meta': {},
                        'id': result["str"][0]["#text"],
                        'source': result["str"][1]["#text"]
                    }
                )
        except KeyError:
            pass
    return formatted_results


def retrieve():
    today = str(date.today())+"T00:00:00Z"
    yesterday = str(date.today() - timedelta(1))+"T00:00:00Z"
    payload = {"api_key": settings.API_KEY, "rows": "0"}
    url = 'http://api.plos.org/search?q=publication_date:[{}%20TO%20{}]'.format(yesterday, today)
    plos_request = requests.get(url, params=payload)
    response = xmltodict.parse(plos_request.text)
    num_article = int(response["response"]["result"]["@numFound"])

    start = 0
    rows = 999

    while rows < num_article + 999:
        payload = {"api_key": settings.API_KEY, "rows": rows, "start": start}
        results = requests.get(url, params=payload)
        tick = time.time()

        formatted_results = parse_response(xmltodict.parse(results.text))
        for result in formatted_results:
            process_docs.process(result)

        start += rows
        rows += rows

        if time.time()-tick < 5:
            time.sleep(5-(time.time()-tick))

    print "Done!"

retrieve()