from lxml import etree
import requests
import json
import datetime
import sys
import re
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
fh = logging.FileHandler('scrAPI.log')
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)

reload(sys)
sys.setdefaultencoding('utf-8')


parse_date_stamp = datetime.date.today().strftime('%Y-%m-%d')


def parse(xml, timestamp, date_stamp=parse_date_stamp):
    """A function for parsing the list of XML objects returned by the consume function. 
    Returns a list of Json objects in a format that can be recognized by the OSF scrapi."""
    terms_url = 'http://purl.org/dc/terms/'
    elements_url = 'http://purl.org/dc/elements/1.1/'
    record = etree.XML(xml)

    contributor_list = record.find(str(etree.QName(elements_url,'creator'))).text.split(';')
    # for now, scitech does not grab emails, but it could soon?
    contributors = []
    for name in contributor_list:
        name = name.strip()
        if name[0] in ['/', ',', 'et. al']:
            continue
        if '[' in name:
            name = name[:name.index('[')].strip()
        contributor = {}
        contributor['full_name'] = name
        contributor['email'] = None
        contributors.append(contributor)

        tags = record.find(str(etree.QName(elements_url, 'subject'))).text
        tags = re.split(',(?!\s\&)|;', tags)
        tags = [tag.strip() for tag in tags]

    json_scrapi = {
        'title': record.find(str(etree.QName(elements_url, 'title'))).text,
        'contributors': contributors,
        'properties': {
            'doi': record.find(str(etree.QName(elements_url,'doi'))).text,
            'description': record.find(str(etree.QName(elements_url,'description'))).text,
            'article_type': record.find(str(etree.QName(elements_url,'type'))).text,
            'url': record.find(str(etree.QName(terms_url,'identifier-purl'))).text,
            'date_entered': record.find(str(etree.QName(elements_url,'dateEntry'))).text,
            'research_org': record.find(str(etree.QName(terms_url,'publisherResearch'))).text,
            'research_sponsor': record.find(str(etree.QName(terms_url, 'publisherSponsor'))).text,
            'tags': tags,
            'date_retrieved': date_stamp,
            'date_published': record.find(str(etree.QName(elements_url, 'date'))).text
        },
        'meta':{},
        'id': record.find(str(etree.QName(elements_url, 'ostiId'))).text,
        'source':'SciTech'
    }
    payload = {
        'doc': json.dumps(json_scrapi),
        'timestamp': timestamp
    }
    return requests.get('http://0.0.0.0:1337/process', params=payload) 


def json_to_text(json_scrapi, date_time=parse_date_stamp):
    """A function to help with debugging. Saves Json produced by parsing XML as a file."""
    with open('SciTech_parsed_'+date_time+'.json', 'w') as json_txt:
        json.dump(json_scrapi, json_txt, indent=4, sort_keys=True)
