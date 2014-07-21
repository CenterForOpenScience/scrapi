from lxml import etree
import requests
import datetime
import sys
import os

reload(sys)
sys.setdefaultencoding('utf-8')


param_date_stamp = datetime.date.today().strftime('%m/%d/%Y')
parse_date_stamp = datetime.date.today().strftime('%Y-%m-%d')


def consume(start_date=param_date_stamp, end_date=None, **kwargs):
    """A function for querying the SciTech Connect database for raw XML. The XML is chunked into smaller pieces, each representing data
    about an article/report. If there are multiple pages of results, this function iterates through all the pages."""

    base_url = 'http://www.osti.gov/scitech/scitechxml'
    parameters = kwargs
    parameters['EntryDateFrom'] = start_date
    parameters['EntryDateTo'] = end_date
    parameters['page'] = 0
    morepages = 'true'
    xml_list = []
    elements_url = 'http://purl.org/dc/elements/1.1/'
    with open(os.path.abspath(os.path.join(os.path.abspath(__file__), os.pardir)) + '/version', 'r') as f:
        version = f.readline()

    while morepages == 'true':
        xml = requests.get(base_url, params=parameters).text
        xml_root = etree.XML(xml.encode('utf-8'))
        for record in xml_root.find('records'):
            payload = [
                etree.tostring(record, encoding='ASCII'),
                'SciTech',
                record.find(str(etree.QName(elements_url, 'ostiId'))).text,
                'xml',
                version
            ]
            xml_list.append(payload)
        parameters['page'] += 1
        morepages = xml_root.find('records').attrib['morepages']
    return xml_list


def xml_to_text(xml_list=None, date_time=parse_date_stamp):
    """A function to help with debugging. Saves consumed XML as a file."""
    with open('SciTech_raw_' + date_time + '.xml', 'w') as raw_xml:
        for record in xml_list:
            raw_xml.write(record)
