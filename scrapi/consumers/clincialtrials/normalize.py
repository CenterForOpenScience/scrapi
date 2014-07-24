import xmltodict
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


def normalize(result, timestamp):

    result = xmltodict.parse(result)

    payload = {
        "doc": {
            'title': result['root']['clinical_study']['brief_title'],
            'contributors': result['root']['clinical_study']['overall_official']['last_name']['#text'],
            'properties': {
                'abstract': result['root']['clinical_study']['brief_summary']
            },
            'meta': {},
            'id': result['root']['clinical_study']['id_info']['nct_id']['#text'],
            'source': "ClinicalTrials.gov",
            'timestamp': str(timestamp)
        }
    }

    return payload

## Tests! ## 
# def test_dict():
#     with open('output.txt', 'r') as f:
#         result = f.read()

#     normed_file = normalize(result, datetime.now())
#     assert isinstance(normed_file, dict)

# def test_contributors():
#     with open('output.txt', 'r') as f:
#         result = f.read()

#     normed_file = normalize(result, datetime.now())
#     assert normed_file['doc']['contributors']
