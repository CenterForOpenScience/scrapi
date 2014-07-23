from datetime import date, timedelta
import time
import os
import xmltodict
import requests
import dicttoxml


def consume():

    """ First, get a list of all recently updated NCTIDs,
    then get the xml one by one and save it into a list 
    of docs including other information """

    today = date.today()
    yesterday = today - timedelta(1)

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
    print count

    url = url + '&count=' + count
    response = get_results_as_dict(url)

    study_urls = []
    for study in response['search_results']['clinical_study']:
        study_urls.append(study['url'] + '?displayxml=true')

    doc_list = []
    for study_url in study_urls[0:3]:
        content = get_results_as_dict(study_url)
        doc_list.append(content)
        # time.sleep(1)

    with open(os.path.abspath(os.path.join(os.path.abspath(__file__), os.pardir)) + '/version', 'r') as f:
        version = f.readline()

    return_list = []
    for doc in doc_list:
        doc_id = doc['clinical_study']['id_info']['nct_id']
        return_list.append((dicttoxml.dicttoxml(doc), 'ClinicalTrials.gov', doc_id, 'xml', version))


    with open('output.txt', 'w') as f:
        f.write(return_list[2][0])

    return return_list


def get_results_as_dict(url):
    results = requests.get(url)
    response = xmltodict.parse(results.text)
    return response


consume()