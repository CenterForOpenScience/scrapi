#!/usr/bin/env python
from datetime import date, timedelta
import time
import xmltodict
import requests
from xml.parsers import expat
from scrapi_tools import lint
from scrapi_tools.document import RawDocument, NormalizedDocument

TODAY = date.today()
YESTERDAY = TODAY - timedelta(1)
NAME = "ClinicalTrials"


def consume():
    """ First, get a list of all recently updated study urls,
    then get the xml one by one and save it into a list 
    of docs including other information """

    month = TODAY.strftime('%m')
    day = TODAY.strftime('%d') 
    year = TODAY.strftime('%Y')

    y_month = YESTERDAY.strftime('%m')
    y_day = YESTERDAY.strftime('%d')
    y_year = YESTERDAY.strftime('%Y')

    base_url = 'http://clinicaltrials.gov/ct2/results?lup_s=' 
    url_end = '{}%2F{}%2F{}%2F&lup_e={}%2F{}%2F{}&displayxml=true'.\
                format(y_month, y_day, y_year, month, day, year)

    url = base_url + url_end
    # print url

    # grab the total number of studies
    initial_request = requests.get(url)
    initial_request = xmltodict.parse(initial_request.text) 
    count = initial_request['search_results']['@count']

    # print 'number of studies this query: ' + str(count)

    xml_list = []
    if int(count) > 0:
        # get a new url with all results in it
        url = url + '&count=' + count
        total_requests = requests.get(url)
        response = xmltodict.parse(total_requests.text)

        # make a list of urls from that full list of studies
        study_urls = []
        for study in response['search_results']['clinical_study']:
            study_urls.append(study['url'] + '?displayxml=true')

        # studies_processed = 0
        # grab each of those urls for full content
        for study_url in study_urls:
            content = requests.get(study_url)
            try:
                xml_doc = xmltodict.parse(content.text)
            except expat.ExpatError:
                print 'xml reading error for ' + study_url
                pass
            doc_id = xml_doc['clinical_study']['id_info']['nct_id']
            xml_list.append(RawDocument({
                    'doc': content.text,
                    'source': NAME,
                    'doc_id': doc_id,
                    'filetype': 'xml',
                }))
            time.sleep(1)
            # studies_processed += 1
            # print 'studies processed: ' + str(studies_processed)

        if int(count) == 0:
            print "No new or updated studies!"
        else: 
            pass

    return xml_list


def normalize(raw_doc, timestamp):
    raw_doc = raw_doc.get('doc')
    try:
        result = xmltodict.parse(raw_doc)
    except expat.ExpatError:
        print 'xml reading error...'
        pass

    try: 
        title = result['clinical_study']['official_title']
    except KeyError:
        try:
            title = result['clinical_study']['brief_title']
        except KeyError:
            title = 'No title available'
            pass

    contributor_list = result['clinical_study'].get('overall_official')

    if not isinstance(contributor_list, list):
        contributor_list = [contributor_list]

    if contributor_list == None:
        contributors = [{'full_name': 'No Contributors', 'email': None}]

    else: 
        contributors = []
        for entry in contributor_list:
            if entry != None:
                name = entry.get('last_name')
                contributor = {}
                contributor['full_name'] = name
                contributor['email'] = None
                contributors.append(contributor)

    try:
        abstract = result['clinical_study']['brief_summary'].get('textblock')
    except KeyError:
        try:
            abstract = result['clinical_study']['detailed_description'].get('textblock')
        except KeyError:
            abstract = 'No abstract available'

    try: 
        nct_id = result['clinical_study']['id_info']['nct_id']
    except KeyError:
        nct_id = 'Secondary ID: ' + result['clinical_study']['id_info']['secondary_id']

    normalized_dict = {
            'title': title,
            'contributors': contributors,
            'properties': {
                'abstract': abstract
            },
            'meta': {},
            'id': nct_id,
            'source': NAME,
            'timestamp': str(timestamp)
    }

    return NormalizedDocument(normalized_dict)



if __name__ == '__main__':
    print(lint(consume, normalize))