from sickle import Sickle 
from sickle.models import Record
from lxml import etree
import xmltodict
import time
import os
from datetime import date, timedelta


def consume():
    doc_list = []
    today = str(date.today())
    yesterday = str(date.today() - timedelta(4))
    sickle = Sickle('http://vtechworks.lib.vt.edu/oai/request')
    records = sickle.ListRecords(
        **{ 'metadataPrefix': 'oai_dc',
        'from': yesterday,
        'ignore_deleted': 'True'
        }
        )

    try:
        for record in records:
            #tree = etree.fromstring(record.raw)
            #print(etree.tostring(tree))
            doc = xmltodict.parse(record.raw)
            print(doc['record']['metadata']['oai_dc:dc']['dc:title'])
    except KeyError:
            print "No new files/updates!"

    with open(os.path.abspath(os.path.join(os.path.abspath(__file__), os.pardir)) + '/version', 'r') as f:
        version = f.readline()

print consume()