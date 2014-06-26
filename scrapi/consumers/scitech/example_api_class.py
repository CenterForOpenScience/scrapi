import os
import requests
import json
import time
import datetime


ScitechData()

ScitechData(3)
ScitechData(SearchFor=3 ost_id=500, 

SciTech.id(input)
parameters['id'] = input 

NPR_API_KEY = os.environ.get('NPR_API_KEY', 'INPUT_NPR_API_KEY_HERE')

#default format for response is json

date_stamp = datetime.datetime.fromtimestamp(time.time()).strftime('%y-%m-%d')

class nprStoryData(object):
	def __init__(self, id=None, numResults=None, requiredAssets=None, date=date_stamp, startDate=None, endDate=None):
		if startDate != None or endDate != None:
			date = None
		self.API_KEY = NPR_API_KEY
		self.url = 'http://api.npr.org/query?apiKey=' + self.API_KEY + '&format=json'
		self.parameters = {'id': id,'numResults': numResults, 'requiredAssets': requiredAssets, 'date': date, 'startDate': startDate, 'endDate':endDate}
		self.json = None
	def get_json(self):
		"""Makes a get request to npr API server and asks for JSON as reply."""
		request = requests.get(self.url, params=self.parameters)
		self.json = request.json()
		return self.json
	def json_to_text(self):
		self.get_json()
		with open ("ID_" + str(self.parameters['id'])+'_'+self.parameters['date']+'_'+'nprStory.json','w') as npr_json:
			json.dump(self.json, npr_json, sort_keys=True, indent=4)

