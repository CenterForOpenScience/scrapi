from datetime import date, timedelta
import xmltodict
import requests


def consume():

    today = date.today()
    yesterday = today - timedelta(4)

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

    url = url + '&count=' + count
    response = get_results_as_dict(url)

    nct_ids = []

    for study in response['search_results']['clinical_study']:
        nct_ids.append(study['nct_id'])

    print nct_ids


def get_results_as_dict(url):
    results = requests.get(url)
    response = xmltodict.parse(results.text)
    return response


consume()