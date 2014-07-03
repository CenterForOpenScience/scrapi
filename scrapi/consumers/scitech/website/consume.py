from lxml import etree
import requests
import json
import datetime
import sys


reload(sys)
sys.setdefaultencoding('utf-8')


param_date_stamp = datetime.date.today().strftime('%m/%d/%Y')
parse_date_stamp = datetime.date.today().strftime('%Y-%m-%d')


def consume(start_date=param_date_stamp, **kwargs):
    """A function for querying the SciTech Connect database for raw XML. The XML is chunked into smaller pieces, each representing data
    about an article/report. If there are multiple pages of results, this function iterates through all the pages."""
    base_url = 'http://www.osti.gov/scitech/scitechxml'
    parameters = kwargs
    parameters['EntryDateFrom'] = start_date
    parameters['page'] = 0
    morepages = 'true'
    xml_list = []
    elements_url ='http://purl.org/dc/elements/1.1/'
    while morepages == 'true':
        xml = requests.get(base_url, params=parameters).text
        xml_root = etree.XML(xml.encode('utf-8'))
        for record in xml_root.find('records'):
            payload = {
                'doc': etree.tostring(record,encoding='ASCII'),
                'source': 'SciTech',
                'doc_id': record.find(str(etree.QName(elements_url, 'ostiId'))).text,
                'filetype': 'xml'
            }
            xml_list.append(payload['doc'])
            requests.get('http://0.0.0.0:1337/process_raw', params=payload)
        parameters['page'] += 1
        morepages = xml_root.find('records').attrib['morepages']
    return xml_list


def xml_to_text(xml_list=None, date_time=parse_date_stamp):
    """A function to help with debugging. Saves consumed XML as a file."""
    with open('SciTech_raw_'+date_time+'.xml', 'w') as raw_xml:
        for record in xml_list:
            raw_xml.write(record)
