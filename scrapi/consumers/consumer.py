__author__ = 'faye'
from scrapi_tools.consumer import BaseConsumer, RawFile, NormalizedFile
import requests
import xmltodict
import json
import time
from datetime import date, timedelta
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import settings
import logging
TODAY = str(date.today()) + "T00:00:00Z"
YESTERDAY = str(date.today() - timedelta(4)) + "T00:00:00Z"
MAX_ROWS_PER_REQUEST = 999

logger = logging.getLogger(__name__)


class PLoSConsumer(BaseConsumer):

    def __init__(self):
        # Nothing to see here
        pass

    def consume(self):
        payload = {"api_key": settings.API_KEY, "rows": "0"}
        base_url = 'http://api.plos.org/search?q=publication_date:[{}%20TO%20{}]'.format(YESTERDAY, TODAY)
        plos_request = requests.get(base_url, params=payload)
        response = xmltodict.parse(plos_request.text)
        num_articles = int(response["response"]["result"]["@numFound"])

        start = 0
        rows = MAX_ROWS_PER_REQUEST
        doc_list = []

        while rows < num_articles + MAX_ROWS_PER_REQUEST:
            payload = {"api_key": settings.API_KEY, "rows": rows, "start": start}
            results = requests.get(base_url, params=payload)
            tick = time.time()

            doc = xmltodict.parse(results.text)

            full_response = doc["response"]["result"]["doc"]

            # TODO Incooporate "Correction" article type
            try:
                for result in full_response:
                    try:
                        if result["arr"][1]["@name"] == "abstract" and result["str"][3]["#text"] == "Research Article":
                            doc_list.append(RawFile({
                                'doc': json.dumps(result, indent=4, sort_keys=True),
                                'source': 'PLoS',
                                'doc_id': result["str"][0]["#text"],
                                'filetype': 'json',
                            }))
                    except KeyError:
                        pass

                start += MAX_ROWS_PER_REQUEST
                rows += MAX_ROWS_PER_REQUEST

                if time.time() - tick < 5:
                    time.sleep(5 - (time.time() - tick))
            except KeyError:
                print "No new files/updates!"
                break

        return doc_list

    def normalize(self, raw_doc, timestamp):
        raw_doc = raw_doc.get('doc')
        try:
            record = json.loads(raw_doc)
        except ValueError as e:
            logger.exception(e)
            return NormalizedFile({
                'title': None,
                'contributors': None,
				'id': None,
				'source': None,
				'timestamp': None,
            })
        return NormalizedFile({
            'title': record["str"][4]["#text"],
            'contributors': [{
                'email': None,
                'full_name': name,
            } for name in record['arr'][0]['str']],
            'properties': {
                'description': record["arr"][1]["str"],
            },
            'meta': {},
            'id': record["str"][0]["#text"],
            'source': "PLoS",
            'timestamp': timestamp
        })
if __name__ == '__main__':
    consumer = PLoSConsumer()
    print(consumer.lint())
