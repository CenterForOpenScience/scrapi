__author__ = 'faye'
import requests
import xmltodict
import time
import json
from datetime import date, timedelta
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
sys.path.insert(1, '/home/faye/cos/scrapi/')
import settings


def consume():
    today = str(date.today()) + "T00:00:00Z"
    yesterday = str(date.today() - timedelta(4)) + "T00:00:00Z"
    payload = {"api_key": settings.API_KEY, "rows": "0"}
    url = 'http://api.plos.org/search?q=publication_date:[{}%20TO%20{}]'.format(yesterday, today)
    plos_request = requests.get(url, params=payload)
    response = xmltodict.parse(plos_request.text)
    num_articles = int(response["response"]["result"]["@numFound"])

    start = 0
    rows = 999
    raw = ""

    while rows < num_articles + 999:
        payload = {"api_key": settings.API_KEY, "rows": rows, "start": start}
        results = requests.get(url, params=payload)
        tick = time.time()

        raw += "\n" + results.text

        doc = xmltodict.parse(results.text)

        for result in doc["response"]["result"]["doc"]:
            try:
                if result["arr"][1]["@name"] == "abstract":
                    payload = {
                        'doc': json.dumps(result, indent=4, sort_keys=True),
                        'source': 'PLoS',
                        'doc_id': result["str"][0]["#text"],
                        'filetype': 'json'
                    }

                    requests.get('http://0.0.0.0:1337/process_raw', params=payload)

            except KeyError:
                pass

        start += rows
        rows += rows

        if time.time() - tick < 5:
            time.sleep(5 - (time.time() - tick))

    return 0
