from datetime import date, timedelta
import time
import os
import xmltodict
import requests
import dicttoxml


def consume():

    """ First, get a list of all recently updated study urls,
    then get the xml one by one and save it into a list 
    of docs including other information """

    today = date.today()
    yesterday = today - timedelta(2)

    month = today.strftime('%m')
    day = today.strftime('%d')
    year = today.strftime('%Y')

    y_month = yesterday.strftime('%m')
    y_day = yesterday.strftime('%d')
    y_year = yesterday.strftime('%Y')

    base_url = 'http://clinicaltrials.gov/ct2/results?lup_s=' 
    url_end = '{}%2F{}%2F{}%2F&lup_e={}%2F{}%2F{}&displayxml=true'.\
                format(y_month, y_day, y_year, month, day, year)

    url = base_url + url_end

    response = get_results_as_dict(url)
    count = response['search_results']['@count']
    return_list = []

    try:
        url = url + '&count=' + count
        response = get_results_as_dict(url)

        study_urls = []
        for study in response['search_results']['clinical_study']:
            study_urls.append(study['url'] + '?displayxml=true')

        doc_list = []
        for study_url in study_urls[0:6]:
            content = requests.get(study_url)
            doc_list.append(content.text)
            time.sleep(1)

        with open(os.path.abspath(os.path.join(os.path.abspath(__file__), os.pardir)) + '/version', 'r') as f:
            version = f.readline()

        for doc in doc_list:
            xml_doc = xmltodict.parse(doc)
            doc_id = xml_doc['clinical_study']['id_info']['nct_id']
            return_list.append((doc, 'ClinicalTrials.gov', doc_id, 'xml', version))

    except KeyError: 
        print "No new files/updates!"

    return return_list


def get_results_as_dict(url):
    results = requests.get(url)
    response = xmltodict.parse(results.text)
    return response

# consume()