import xmltodict
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


def normalize(result, timestamp):

    result = xmltodict.parse(result)

    payload = {
        "doc": {
            'title': result['clinical_study']['brief_title'],
            'contributors': result['clinical_study']['overall_official']['last_name'],
            'properties': {
                'abstract': result['clinical_study']['brief_summary']['textblock']
            },
            'meta': {},
            'id': result['clinical_study']['id_info']['nct_id'],
            'source': "ClinicalTrials.gov",
            'timestamp': str(timestamp)
        }
    }

    return payload


## TODO: fix contributors to be a list of dicts, maybe? To match others... 