from __future__ import unicode_literals

from scrapi.base.schemas import BASEXMLSCHEMA, update_schema

RAW_DOC = {
    'doc': str('{}'),
    'docID': 'someID',
    'timestamps': {
        'harvestFinished': '2012-11-30T17:05:48+00:00',
        'harvestStarted': '2012-11-30T17:05:48+00:00',
        'harvestTaskCreated': '2012-11-30T17:05:48+00:00'
    },
    'filetype': 'json',
    'source': 'tests'
}

RECORD = {
    'title': 'Using Table Stable Carbon in Gold and STAR Isotopes',
    'contributors': [
        {
            'prefix': 'The One And Only',
            'given': 'DEVON',
            'middle': 'Get The Tables',
            'family': 'DUDLEY',
            'suffix': 'Thirsty Boy',
            'email': 'dudley.boyz@email.uni.edu',
            'ORCID': 'BubbaRayDudley'
        }
    ],
    'id': {
        'url': 'http://www.plosone.org/article',
        'doi': '10.1371/doi.DOI!',
        'serviceID': 'AWESOME'
    },
    'properties': {
        'figures': ['http://www.plosone.org/article/image.png'],
        'type': 'text',
                'yep': 'A property'
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
    'dateUpdated': '2015-02-23T17:05:48+00:01',
    '_id': 'yes! yes! yes!',
    'count': 0
}


TEST_SCHEMA = update_schema(BASEXMLSCHEMA, {
    "title": ("//dc:title/node()", lambda x: "Title overwritten"),
    "properties": {
        "title1": "//dc:title/node()",
        "title2": ["//dc:title/node()", lambda x: x.lower()],
        "title3": ["//dc:title/node()", "//dc:title/node()", lambda x, y: x + y.lower()]
    }
})


def get_leaves(d, leaves=None):
    if leaves is None:
        leaves = []

    for k, v in d.items():
        if isinstance(v, dict):
            leaves.extend(get_leaves(v, leaves))
        else:
            leaves.append((k, v))

    return leaves


TEST_NAMESPACES = {
    'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
    'dc': 'http://purl.org/dc/elements/1.1/',
    'dcq': 'http://purl.org/dc/terms/'
}


TEST_XML_DOC = '''
    <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcq="http://purl.org/dc/terms/">
        <records count="97" morepages="true" start="1" end="10">
            <record rownumber="1">
                <dc:title>Test</dc:title>
                <dc:creator>
                Raveh-Sadka, Tali; Thomas, Brian C; Singh, Andrea; Firek, Brian; Brooks,
                Brandon; Castelle, Cindy J; Sharon, Itai; Baker, Robyn; Good, Misty; Morowitz,
                Michael J; Banfield, Jillian F
                </dc:creator>
                <dc:subject/>
                <dc:subjectRelated/>
                <dc:description/>
                <dcq:publisher>
                    eLife Sciences Publications, Ltd.
                </dcq:publisher>
                <dcq:publisherAvailability/>
                <dcq:publisherResearch>
                    None
                </dcq:publisherResearch>
                <dcq:publisherSponsor>
                    USDOE
                </dcq:publisherSponsor>
                <dcq:publisherCountry>
                    Country unknown/Code not available
                </dcq:publisherCountry>
                <dc:date>
                    2015-03-03
                </dc:date>
                <dc:language>
                    English
                </dc:language>
                <dc:type>
                    Journal Article
                </dc:type>
                <dcq:typeQualifier/>
                <dc:relation>
                    Journal Name: eLife; Journal Volume: 4
                </dc:relation>
                <dc:coverage/>
                <dc:format>
                    Medium: X
                </dc:format>
                <dc:identifier>
                    OSTI ID: 1171761, Legacy ID: OSTI ID: 1171761
                </dc:identifier>
                <dc:identifierReport>
                    None
                </dc:identifierReport>
                <dcq:identifierDOEcontract>
                    5R01AI092531; Long term fellowship; SC0004918; ER65561; APSF-2012-10-05
                </dcq:identifierDOEcontract>
                <dc:identifierOther>Journal ID: ISSN 2050-084X</dc:identifierOther>
                <dc:doi>10.7554/eLife.05477</dc:doi><dc:rights/>
                <dc:dateEntry>2015-03-05</dc:dateEntry>
                <dc:dateAdded>2015-03-05</dc:dateAdded>
                <dc:ostiId>1171761</dc:ostiId>
                <dcq:identifier-purl type=""/>
                <dcq:identifier-citation>
                    http://www.osti.gov/pages/biblio/1171761
                </dcq:identifier-citation>
            </record>
        </records>
    </rdf:RDF>
'''
