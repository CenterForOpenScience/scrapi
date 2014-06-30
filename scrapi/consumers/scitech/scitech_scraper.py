from lxml import etree
import requests
import json
import datetime
import time


date_stamp = datetime.date.today().strftime('%m/%d/%Y')

class SciTechScraper(object):
	def __init__(self, date_time=date_stamp, **kwargs):
		self.base_url = 'http://www.osti.gov/scitech/scitechxml'
		self.parameters = kwargs
		self.xml = None
		self.url = None
		self.date_stamp = date_time
	def consume(self):
		self.parameters['EntryDateFrom'] = self.date_stamp
		req = requests.get(self.base_url, params=self.parameters)
		self.xml = req.text
		self.url = req.url
		return self.xml
	def xml_to_text(self):
		if not self.xml:
			self.consume()
		with open('SciTech_raw_xml_' + '.xml', 'w') as raw_xml:
			raw_xml.write(self.xml.encode('utf-8'))
	def parse(self):
		if not self.xml:
			self.consume()
		xml_root = etree.fromstring(self.xml.encode('utf-8'))
		json_scrapi_list = []
		terms_url = 'http://purl.org/dc/terms/'
		elements_url = 'http://purl.org/dc/elements/1.1/'
		for record in xml_root.find('records'):
			json_scrapi = {
				'title': record.find(str(etree.QName(elements_url, 'title'))).text,
				'contributors': record.find(str(etree.QName(elements_url,'creator'))).text,
				'properties': {
					'doi': record.find(str(etree.QName(elements_url,'doi'))).text,
					'description': record.find(str(etree.QName(elements_url,'description'))).text,
					'article_type': record.find(str(etree.QName(elements_url,'type'))).text,
					'url': record.find(str(etree.QName(terms_url,'identifier-purl'))).text,
					'date_entered': record.find(str(etree.QName(elements_url,'dateEntry'))).text,
					'date_retrieved': self.date_stamp
				},
				'id': record.find(str(etree.QName(elements_url, 'ostiId'))).text,
				'source':'SciTech Connect'
			}
			json_scrapi_list.append(json_scrapi)
			with open('test.json', 'w') as test:
				json.dump({'studies': json_scrapi_list}, test, indent=4, sort_keys=True)
		return json_scrapi_list