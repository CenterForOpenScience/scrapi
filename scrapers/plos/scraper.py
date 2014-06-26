__author__ = 'faye'
import requests
import xmltodict
import json
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

def retrieve():
    payload = {"api_key": "7AbMQ-D-3hKy5hJ1FPY2", "rows": "0"}
    plos_request = requests.get('http://api.plos.org/search?q=publication_date:[2014-06-23T00:00:00Z%20TO%202014-06-24T00:00:00Z]', params=payload)
    dictionary = xmltodict.parse(plos_request.text)
    print dictionary["response"]["result"]["@numFound"]

    #process_docs.process_raw(text, 'basic_scraper', '10.1371%2Fjournal.pbio.1001356', 'xml')
    #parse(text)

retrieve()