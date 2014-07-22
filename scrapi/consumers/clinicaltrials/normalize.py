import sys
import xmltodict
from bs4 import BeautifulSoup
reload(sys)
sys.setdefaultencoding('utf-8')


def normalize(result, timestamp):
    result = xmltodict.parse(result)
    payload = {
        "doc": {
            'title': result["str"][4]["#text"],
            'contributors': result["arr"][0]["str"],
            'properties': {
        'abstract': result["arr"][1]["str"]
            },
            'meta': {},
            'id': result["str"][0]["#text"],
            'source': "ClinicalTrials.gov",
            'timestamp': str(timestamp)
        }
    }
    return payload



def normalize(nct_id, timestamp):
    """Takes a clinicaltrials.gov nct_id and returns json in a format containing info about the trial in a format
    that can be imported by scrAPI."""
    
    files = set([f.rstrip('-before').rstrip('-after') for f in glob('files/{0}/*.xml'.format(nct_id))])
    files = sorted(files, key=lambda v: time.mktime(time.strptime(v.split('/')[-1].rstrip('.xml').split('_')[-1], '%Y%m%d')))


    if len(files) == 0:
        return None
    
    trial = parse('xml/{0}.xml'.format(nct_id))

    versions = {}
    for f in files:
        version = f.split('/')[-1].rstrip('.xml').split('_')[-1]
        v, locations = xml_to_json(f)
        v['clinical_study']['location'] = l2c(locations)
        v['clinical_study']['references'] = add_pubmed_to_references(v)
        v['clinical_study']['keyword'] = ([v['clinical_study'].get('keyword')] or [])+trial['keywords']
        add_pubmed_to_references(v)
        versions[version] = v


        payload = {
        "doc": {
            'title': trial["title"],
            'contributors': trial["contributors"],
            'properties': {
                'abstract': trial["description"]
            },
            'meta': {},
            'id': trial["id"],
            'source': "ClinicalTrials.gov",
            'timestamp': str(timestamp)
        }
    }
        
    return payload