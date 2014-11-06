# This is code to simulate someome pushing to SCRAPI!

import json
import requests

# The fake data they're seding in a list of documents


def generate_events():
    '''
    returns a list of dicts with all the stuff in it 
    for a proper scrAPI request! 
    '''
    payload = {
        'source': 'Wrestling Digest',
        'events': [
            {
                'title': 'Using RING!! Stable Carbon in Gold Isotopes',
                 'contributors': [
                    {
                        'prefix': 'Mr',
                        'given': 'STAR',
                        'middle': 'G',
                        'family': 'DUST!',
                        'suffix': 'II',
                        'email': 'dustin.runnels@email.uni.edu',
                        'ORCID': 'add-ORCID-here'
                    }
                ],
                'id': {
                    'url': 'http://www.plosone.org/article',
                    'doi': '10.1371/doi.DOI!',
                    'serviceID': '10.1371/doi'
                },
                'properties': {
                    'figures': ['http://www.plosone.org/article/image.png'],
                    'type': 'text',
                    'yep':'A property'
                },
                'description': 'This study seeks to understand how humans impact\
                the dietary patterns of eight free-ranging vervet monkey\
                (Chlorocebus pygerythrus) groups in South Africa using stable\
                isotope analysis.',
                'tags': [
                    'behavior',
                    'genetics'
                ],
                'source': 'example_pusher',
                'dateCreated': '2012-11-30T17:05:48+00:00',
                'dateUpdated': '2015-01-01T17:05:48+00:00'
            }
        ]
    }

    return payload


def send_post_to_scrapi(events):

    url_for_post = 'http://localhost:1337/api/v1/SHARE/'

    posted = requests.post(url_for_post, data=json.dumps(events), auth=('Wrestling Digest', 'lunaticfringe4life'))


send_post_to_scrapi(generate_events())
