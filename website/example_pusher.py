## This is code to simulate someome pushing to SCRAPI! 

import requests

# The fake data they're seding - just one report

def generate_one_event():
    '''
    returns a single well_formatted dict with all the stuff in it 
    for one proper scrAPI request! 
    '''
    return {
        'title': 'PLOS ONE: Using Ring Stable Carbon in Gold Isotopes',
        'contributors': [
            {
                'prefix': 'Mr',
                'given': 'Dustin',
                'middle': 'G',
                'family': 'Runnels',
                'suffix': 'II',
                'email': 'dustin.runnels@email.uni.edu',
                'ORCID': 'add-ORCID-here'
            }
        ],
        'id': {
            'url': 'http://www.plosone.org/article', 
            'doi': '10.1371/doi', 
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
        'dateCreated': '2014-05-30T17:05:48+00:00',
        'dateUpdated': '2014-05-30T17:05:48+00:00',
    }

def send_post_to_scrapi(event):

    url_for_post = 'http://localhost:1337/api/v1/SHARE/?events={}'.format(event)

    posted = requests.post(url_for_post)
    import pdb; pdb.set_trace()


send_post_to_scrapi(generate_one_event())
