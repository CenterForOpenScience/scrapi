from lxml import etree
import requests
import json
import datetime


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
	while morepages == 'true':
		xml = requests.get(base_url, params=parameters).text
		xml_root = etree.fromstring(xml.encode('utf-8'))
		for record in xml_root.find('records'):
			xml_list.append(record)
		parameters['page'] += 1
		morepages = xml_root.find('records').attrib['morepages']
	return xml_list


def parse(xml_list=None, date_stamp=parse_date_stamp):
	"""A function for parsing the list of XML objects returned by the consume function. 
	Returns a list of Json objects in a format that can be recognized by the OSF scrapi."""
	if not xml_list:
		xml_list= consume()
	json_scrapi_list = []
	terms_url = 'http://purl.org/dc/terms/'
	elements_url = 'http://purl.org/dc/elements/1.1/'
	for record in xml_list:
		json_scrapi = {
			'title': record.find(str(etree.QName(elements_url, 'title'))).text,
			'contributors': record.find(str(etree.QName(elements_url,'creator'))).text,
			'properties': {
				'doi': record.find(str(etree.QName(elements_url,'doi'))).text,
				'description': record.find(str(etree.QName(elements_url,'description'))).text,
				'article_type': record.find(str(etree.QName(elements_url,'type'))).text,
				'url': record.find(str(etree.QName(terms_url,'identifier-purl'))).text,
				'date_entered': record.find(str(etree.QName(elements_url,'dateEntry'))).text,
				'date_retrieved': date_stamp
			},
			'id': record.find(str(etree.QName(elements_url, 'ostiId'))).text,
			'source':'SciTech Connect'
		}
		json_scrapi_list.append(json_scrapi)
	return json_scrapi_list


def xml_to_text(xml_list=None, date_time=parse_date_stamp):
	"""A function to help with debugging. Saves consumed XML as a file."""
	if not xml_list:
		xml_list = consume()
	with open('SciTech_raw_xml_'+date_time+'.xml', 'w') as raw_xml:
		for record in xml_list:
			raw_xml.write(etree.tostring(record,encoding='utf-8'))


def json_to_text(json_scrapi=None, date_time=parse_date_stamp):
	"""A function to help with debugging. Saves Json produced by parsing XML as a file."""
	if not json_scrapi:
		json_scrapi = parse()
	with open('SciTech_parsed_json_'+date_time+'.json', 'w') as test:
		json.dump(json_scrapi_list, test, indent=4, sort_keys=True)
