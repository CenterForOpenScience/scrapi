# -*- coding: utf-8 -*-

from sickle import Sickle 
from sickle.models import Record
from lxml import etree
import xmltodict
import dicttoxml
import time
import os
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from datetime import date, timedelta
import json

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

    # TODO: This assumes that an empty records collection throws a `KeyError`. This should be verified.
    try:
        for record in records:
            #tree = etree.fromstring(record.raw)
            #print(etree.tostring(tree))
            doc = xmltodict.parse(record.raw)
                            #f.write(json.dumps(doc, indent=2))
            # printing the titles, to show that it's all working
            # print(doc['record']['metadata']['oai_dc:dc']['dc:title'])
            try:
                doc_list.append(doc['record']) # should grab the header AND metadata, skip XML version
            except KeyError:
                pass
    except KeyError:
            print "No new files/updates!"

    with open(os.path.abspath(os.path.join(os.path.abspath(__file__), os.pardir)) + '/version', 'r') as f:
        version = f.readline()

    return_list = []
    test_list = []
    for x in range(0, len(doc_list)):
        doc_xml = doc_list[x]
        doc_id = doc_xml['header']['identifier']
        return_list.append((dicttoxml.dicttoxml(doc_list[x]), 'VTechWorks', doc_id, 'xml', version))

# UNCOMMENT TO RUN TESTS IN NORMALIZE.PY
#        test_list.append(dicttoxml.dicttoxml(doc_list[x]))

#    with open('output.xml','w') as f:
#        f.write(test_list[1])

    return return_list

print consume()
