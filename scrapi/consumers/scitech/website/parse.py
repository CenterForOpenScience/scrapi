from lxml import etree
import requests
import json
import datetime
import sys

reload(sys)
sys.setdefaultencoding('utf-8')


parse_date_stamp = datetime.date.today().strftime('%Y-%m-%d')


def parse(xml, timestamp, date_stamp=parse_date_stamp):
	"""A function for parsing the list of XML objects returned by the consume function. 
	Returns a list of Json objects in a format that can be recognized by the OSF scrapi."""
#	json_scrapi_list = []
	terms_url = 'http://purl.org/dc/terms/'
	elements_url = 'http://purl.org/dc/elements/1.1/'
#	for record in xml:
	record = etree.fromstring(xml)
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
		'meta':{},
		'id': record.find(str(etree.QName(elements_url, 'ostiId'))).text,
		'source':'SciTech Connect'
	}
	payload = {
		'doc': json.dumps(json_scrapi),
		'timestamp': timestamp
	}
#	json_scrapi_list.append(json_scrapi)
#   return json_scrapi_list
	return requests.get('http://0.0.0.0:1337/process', params=payload) 


def json_to_text(json_scrapi, date_time=parse_date_stamp):
	"""A function to help with debugging. Saves Json produced by parsing XML as a file."""
	with open('SciTech_parsed_'+date_time+'.json', 'w') as json_txt:
		json.dump(json_scrapi, json_txt, indent=4, sort_keys=True)
