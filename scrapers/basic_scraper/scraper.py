import sys

reload(sys)
sys.setdefaultencoding('utf-8')
from lxml import etree
from api import process_docs

def retrieve():
    with open('10.1371%2Fjournal.pbio.1001356.xml') as f:
        text = f.read()

    process_docs.process_raw(text, 'basic_scraper', '10.1371%2Fjournal.pbio.1001356', 'xml')
    parse(text)

def parse(text):
    doc = etree.fromstring(text)
    str_list = doc.findall('str')
    for item in str_list:
        print str(item.attrib) + '\n\t' + item.text.strip()
    arr_list = doc.findall('arr')
    for item in arr_list:
        str_arr_list = item.findall('str')
        for str_item in str_arr_list:
            print str(item.attrib) + '\n\t' + str_item.text.strip()
retrieve()