#!/usr/bin/env python
from datetime import date, timedelta
import time
import xmltodict
import requests
from lxml import etree
from lxml import objectify
from xml.parsers import expat
from scrapi_tools import lint
from scrapi_tools.document import RawDocument, NormalizedDocument

TODAY = date.today()
NAME = "ClinicalTrials"

def consume(days_back=1):
    """ First, get a list of all recently updated study urls,
    then get the xml one by one and save it into a list 
    of docs including other information """

    start_date = TODAY - timedelta(days_back)

    month = TODAY.strftime('%m')
    day = TODAY.strftime('%d') 
    year = TODAY.strftime('%Y')

    y_month = start_date.strftime('%m')
    y_day = start_date.strftime('%d')
    y_year = start_date.strftime('%Y')

    base_url = 'http://clinicaltrials.gov/ct2/results?lup_s=' 
    url_end = '{}%2F{}%2F{}%2F&lup_e={}%2F{}%2F{}&displayxml=true'.\
                format(y_month, y_day, y_year, month, day, year)

    url = base_url + url_end

    # grab the total number of studies
    initial_request = requests.get(url)
    initial_request = xmltodict.parse(initial_request.text) 
    count = initial_request['search_results']['@count']

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

        # grab each of those urls for full content
        for study_url in study_urls[:5]:
            content = requests.get(study_url)
            try:
                xml_doc = xmltodict.parse(content.text)
            except expat.ExpatError:
                print 'xml reading error for ' + study_url
                pass
            doc_id = xml_doc['clinical_study']['id_info']['nct_id']
            xml_list.append(RawDocument({
                    'doc': content.content,
                    'source': NAME,
                    'doc_id': doc_id,
                    'filetype': 'xml',
                }))
            time.sleep(1)

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

    xml_doc = etree.XML(raw_doc)
    xml_object = objectify.XML(raw_doc)

    # Title
    try: 
        title = result['clinical_study']['official_title']
    except KeyError:
        try:
            title = result['clinical_study']['brief_title']
        except KeyError:
            title = 'No title available'
            pass

    # contributors
    contributor_list = xml_doc.xpath('//overall_official/last_name/node()') or xml_doc.xpath('//lead_sponsor/agency/node()') or ['No contributors']
    contributors = [{'full_name': contributor_list[0], 'email': ''}]

    # abstract
    try:
        abstract = result['clinical_study']['brief_summary'].get('textblock')
    except KeyError:
        try:
            abstract = result['clinical_study']['detailed_description'].get('textblock')
        except KeyError:
            abstract = 'No abstract available'

    # IDs
    try: 
        nct_id = result['clinical_study']['id_info']['nct_id']
    except KeyError:
        nct_id = 'Secondary ID: ' + result['clinical_study']['id_info'].get('secondary_id')
    url = result['clinical_study']['required_header'].get('url')
    ids = {'service_id': nct_id, 'doi': '', 'url': url}

    # date created
    date_created = result['clinical_study'].get('firstreceived_date')

    # tags/keywords
    keywords = xml_doc.xpath('//keyword/node()')

    lead_sponsor  = {
            'agency': (xml_doc.xpath('//lead_sponsor/agency/node()') or [''])[0],
            'agency_class': (xml_doc.xpath('//lead_sponsor/agency_class/node()') or [''])[0]
        }

    primary_outcome = {
            'measure': (xml_doc.xpath('//primary_outcome/measure/node()') or [''])[0],
            'time_frame': (xml_doc.xpath('//primary_outcome/time_frame/node()') or [''])[0],
            'safety_issue': (xml_doc.xpath('//primary_outcome/safety_issue/node()') or [''])[0]
        }


    # gives a list of dictionaries of all secondary outcomes
    secondary_outcome_elements = xml_doc.xpath('//secondary_outcome')
    secondary_outcomes = []
    for item in secondary_outcome_elements:
        secondary_outcome = {element.tag: element.text for element in item.iterdescendants()}
        secondary_outcomes.append(secondary_outcome)

    # enrollment - can have different types
    enrollment_list = xml_doc.xpath('//enrollment')
    enrollment = {item.values()[0]: item.text for item in enrollment_list}

    # arm group
    arm_group_elements = xml_doc.xpath('//arm_group')
    arm_groups = []
    for item in arm_group_elements:
        arm_group = {element.tag: element.text for element in item.iterdescendants()}
        arm_groups.append(arm_group)

    # intervention
    intervention_elements = xml_doc.xpath('//intervention')
    interventions = []
    for item in intervention_elements:
        intervention = {element.tag: element.text for element in item.iterdescendants()}
        interventions.append(intervention)

    # eligibility
    eligibility_elements = xml_doc.xpath('//eligibility')

    eligibility = {}
    for element in eligibility_elements[0].iterchildren():
        if element.text.strip() == '':
            for child in element.getchildren():
                if child.text.strip() != '':               
                    eligibility[element.tag] = child.text
        else:
            eligibility[element.tag] = element.text

    ## TODO: location - undone for now
    ## location has a facility name - and address with seperate city state zip country
    ## {name: 'facility name', address: {city: 'city', state:'state', zip: 'zip', country:'country'}, status: 'status'}

    # location_elements = xml_doc.xpath('//location')
    # locations = []
    # upper_level = [item.getchildren() for item in location_elements]
    # location = {}
    # for item in upper_level:
    #     for element in item:
    #         for detail in element:
    #             location[element.tag] = {detail.tag: detail.text}

    # link
    link_elements = xml_doc.xpath('//link')
    links = []
    for item in link_elements:
        link = {element.tag: element.text for element in item.iterdescendants()}
        links.append(intervention)

    # responsible party 
    # TODO: is there ever more than one responsible party? 
    responsible_party_elements = xml_doc.xpath('//responsible_party')
    try:
        responsible_party = {elem.tag: elem.text for elem in responsible_party_elements[0].iterdescendants()}
    except IndexError:
        responsible_party = {}

    #TODO: a more robust search for more metadata in different examples

    ## extra properties ##
    properties = {
        'sponsors': lead_sponsor,
        'oversight_authority': xml_doc.xpath('//oversigh_info/authority/node()'),
        'study_design': (xml_doc.xpath('//study_design/node') or [''])[0],
        'primary_outcome': primary_outcome,
        'secondary_outcomes': secondary_outcomes,
        'number_of_arms' : (xml_doc.xpath('//number_of_arms/node()') or [''])[0],
        'enrollment': enrollment,
        'source': (xml_doc.xpath('//source/node()') or [''])[0],
        'condition': (xml_doc.xpath('//condition/node()') or [''])[0], 
        'arm_group' : arm_groups, 
        'intervention': interventions,
        'eligibility': eligibility,
        'link' : links,
        'verification_date': (xml_doc.xpath('//verification_date/node()') or [''])[0],
        'last_changed': (xml_doc.xpath('//lastchanged_date/node()') or [''])[0],
        'responsible_party' : responsible_party,
        'status': (xml_doc.xpath('//status/node()') or [''])[0],
        'location_countries': xml_doc.xpath('//location_countries/country/node()'),
        'is_fda_regulated' : (xml_doc.xpath('//is_fda_regulated/node()') or [''])[0],
        'is_section_801': (xml_doc.xpath('//is_section_801/node()') or [''])[0],
        'has_expanded_access': (xml_doc.xpath('//has_expanded_access/node()') or [''])[0]
    }

    normalized_dict = {
            'title': title,
            'contributors': contributors,
            'properties': properties,
            'description': abstract,
            'meta': {},
            'id': ids,
            'source': NAME,
            'tags': keywords,
            'date_created': date_created,
            'timestamp': str(timestamp)
    }

    return NormalizedDocument(normalized_dict)


if __name__ == '__main__':
    print(lint(consume, normalize))