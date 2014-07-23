import xmltodict
from lxml import etree


def parse(result, timestamp):
    """Used to extract information from an XML document from clinicaltrials.gov 
    and to return Json for scrAPI """

    payload = {}
    root = etree.parse(result).getroot()
    doc = {}

    if root.find('official_title') == None:
        doc['title'] = root.find('brief_title').text
    else:
        doc['title'] = root.find('official_title').text

    doc['contributors'] = []
    for entry in root.findall('overall_official'):
        official_dict = {}
        official_dict['full_name'] = entry.find('last_name').text
        official_dict['email'] = None
        doc['contributors'].append(official_dict)

    properties = {}
    properties['abstract'] = root.find('brief_summary')[0].text
    doc['properties'] = properties

    doc['meta'] = {}

    doc['id'] = root.find('id_info').find('nct_id').text

    doc['timestamp'] = str(timestamp)

    doc['source'] = 'ClinicalTrials.gov'

    payload['doc'] = doc

    return payload