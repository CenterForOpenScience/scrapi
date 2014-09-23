''' Consumer for OpenSUIC - Southern Illinios University '''

import time
from lxml import etree
from datetime import date, timedelta

import requests
from scrapi_tools import lint
from scrapi_tools.document import RawDocument, NormalizedDocument

TODAY = date.today()
NAME = "StCloudState"
OAI_DC_BASE = 'http://opensiuc.lib.siu.edu/do/oai/'

NAMESPACES = {'dc': 'http://purl.org/dc/elements/1.1/', 
            'oai_dc': 'http://www.openarchives.org/OAI/2.0/',
            'ns0': 'http://www.openarchives.org/OAI/2.0/'}

def consume(days_back=5):
    start_date = TODAY - timedelta(days_back)
    base_url = OAI_DC_BASE + '?verb=ListRecords&metadataPrefix=oai_dc&from='
    url = base_url + str(start_date) + 'T00:00:00Z'
    print url

    records = get_records(url)

    xml_list = []
    for record in records:
        set_spec = record.xpath('ns0:header/ns0:setSpec/node()', namespaces=NAMESPACES)[0]
        doc_id = record.xpath('ns0:header/ns0:identifier/node()', namespaces=NAMESPACES)[0]
        record_string = etree.tostring(record)
        record_string = '<?xml version="1.0" encoding="UTF-8"?>\n' + record_string

        # if set_spec.replace('publication:', '') in series_name_list:
        xml_list.append(RawDocument({
                    'doc': record_string,
                    'source': NAME,
                    'doc_id': doc_id,
                    'filetype': 'xml'
                }))

    return xml_list

def get_records(url):
    data = requests.get(url)
    doc = etree.XML(data.content)
    records = doc.xpath('//ns0:record', namespaces=NAMESPACES)
    token = doc.xpath('//ns0:resumptionToken/node()', namespaces=NAMESPACES)

    if len(token) == 1:
        time.sleep(0.5)
        base_url = OAI_DC_BASE + '?verb=ListRecords&resumptionToken=' 
        url = base_url + token[0]
        records += get_records(url)

    return records

    ## TODO: fix if there are no records found... what would the XML look like?

def normalize(raw_doc, timestamp):
    doc = raw_doc.get('doc')

    record = etree.XML(doc)

    # # save the list of approved series names as a big list - first option
    # series_name_list = ['acb_frp', 'agtc', 'anthrofrp', 'bio_fuel', 'biomed_eng_frp', 'biomedcentral', 'biosci_frp', 'business_frp', 'ce_eng_frp', 'chemfrp', 'cjfrp', 'cmmg', 'coe_aos', 'coe_khs', 'coe_tbf', 'coe_ted', 'commfrp', 'commsci_frp', 'compscifrp', 'cpcs_pubs', 'csdt', 'ec_eng_frp', 'englishfrp', 'geofrp', 'gerontology', 'humbiol_preprints', 'iehs', 'im_eng_frp', 'immunology_frp', 'libsp', 'mathfrp', 'med_anesthesiology', 'med_biochem', 'med_cardio', 'med_cher', 'med_dermatology', 'med_dho', 'med_did', 'med_dpacs', 'med_edm', 'med_em', 'med_intmed', 'med_neurology', 'med_neurosurgery', 'med_obgyn', 'med_ohn_surgery', 'med_oncology', 'med_opthalmology', 'med_ortho_surgery', 'med_path', 'med_pbn', 'med_pediatrics', 'med_pmr', 'med_radiology', 'med_ro', 'med_surgery', 'med_urology', 'mott_pubs', 'musicfrp', 'nfsfrp', 'nursingfrp', 'pet', 'pharm_appsci', 'pharm_healthcare', 'pharm_practice', 'pharm_science', 'pharma_frp', 'philofrp', 'phy_astro_frp', 'physio_frp', 'prb', 'provost_pub', 'psychfrp', 'skillman', 'slisfrp', 'soc_work_pubs', 'socfrp', 'urbstud_frp', 'antipodes', 'criticism', 'discourse', 'framework', 'humbiol', 'jewishfilm', 'jmasm', 'marvels', 'mpq', 'narrative', 'storytelling']
    
    # # # load the list of approved series_names as a file - second option
    # # with open('series_names.txt') as series_names:
    # #     series_name_list = [word.replace('\n', '') for word in series_names]
    # set_spec = record.xpath('ns0:header/ns0:setSpec/node()', namespaces=NAMESPACES)[0]

    # if set_spec.replace('publication:', '') not in series_name_list:
    #     print 'Series not in approved list, not normalizing...'
    #     return None

    # contributors #
    contributors = record.xpath('//dc:creator', namespaces=NAMESPACES)
    contributor_list = []
    for contributor in contributors:
        contributor_list.append({'full_name': contributor.text, 'email':''})
    
    # title
    title = record.xpath('//dc:title/node()', namespaces=NAMESPACES)[0]

    # ids
    service_id = record.xpath('ns0:header/ns0:identifier', namespaces=NAMESPACES)[0].text

    all_ids = record.xpath('//dc:identifier/node()', namespaces=NAMESPACES)
    pdf = ''
    for identifier in all_ids:
        if 'cgi/viewcontent' not in identifier and OAI_DC_BASE[:-7] in identifier:
            url = identifier
        if 'cgi/viewcontent' in identifier:
            pdf = identifier

    ids = {'url': url, 'service_id': service_id, 'doi': ''}

    # description
    description = (record.xpath('//dc:description/node()', namespaces=NAMESPACES) or [''])[0]

    # Earliest date - original date - - most only have one date - so this is simple!
    date_created = (record.xpath('ns0:metadata/oai_dc:dc/dc:date/node()', namespaces=NAMESPACES) or [''])[0]

    # tags
    tags = record.xpath('//dc:subject/node()', namespaces=NAMESPACES)

    #properties (publisher, source, type, format, date, pdf, all ids)
    properties = {}
    properties["publisher"] = (record.xpath('//dc:publisher/node()', namespaces=NAMESPACES) or [''])[0]
    properties["source"] = (record.xpath('//dc:source/node()', namespaces=NAMESPACES) or [''])[0]
    properties["type"] = (record.xpath('//dc:type/node()', namespaces=NAMESPACES) or [''])[0]
    properties["format"] = (record.xpath('//dc:format/node()', namespaces=NAMESPACES) or [''])[0]
    properties["date"] = (record.xpath('//dc:date/node()', namespaces=NAMESPACES) or [''])[0]
    properties["pdf_download"] = pdf
    properties['identifiers'] = all_ids

    normalized_dict = {
            'title': title,
            'contributors': contributor_list,
            'properties': properties,
            'description': description,
            'meta': {},
            'id': ids,
            'tags': tags,
            'source': NAME,
            'date_created': date_created,
            'timestamp': str(timestamp)
    }

    return NormalizedDocument(normalized_dict)
        

if __name__ == '__main__':
    print(lint(consume, normalize))
