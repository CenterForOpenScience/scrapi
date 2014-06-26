# This file will be used to process raw and preprocessed-JSON documents from the scrapers
import os
import json


def process_raw(doc, source, id, filetype):
    """
        Takes a document (in the form of text, and saves it to disk
        with the specified name and the designated filetype in the
        specified source directory
    """
    directory = '/home/fabian/cos/scrapi/api/raw/' + str(source) + '/'
    filepath = directory + str(id) + '.' + str(filetype)
    if not os.path.exists(directory):
        os.makedirs(directory)

    with open(filepath, 'w') as f:
        f.write(doc)


def process(doc):
    """
        Takes a JSON document and extracts the information necessary
        to make an OSF project, then creates that OSF project through
        the API (does not exist yet)
        Format specification:
        {
            'title': {PROJECT_TITLE},
            'contributors: [{PROJECT_CONTRIBUTORS}],
            'properties': {
                {VALID_NODE_PROPERTY}: {NODE_PROPERTY_VALUE},
            },
            'meta': {META_DATA FOR PROJECT | EMPTY},
            'id': {DOI OR UNIQUE ID OF PROJECT},
            'source': {SOURCE OF SCRAPE}
        }
    """

    directory = '/home/fabian/cos/scrapi/api/json/' + doc['source'] + '/'
    filepath = directory + doc['id'].replace('/', '%2F') + '.json'
    if not os.path.exists(directory):
        os.makedirs(directory)
    with open(filepath, 'w') as f:
        f.write(json.dumps(doc, sort_keys=True, indent=4))
    node = {}
    node['title'] = doc['title']
    node['contributors'] = doc['contributors']

    properties = doc['properties']
    for property in properties.keys():
        if property in ['description', 'tags', 'system_tags', 'wiki_pages_current']:
            node[property] = properties[property]

    print(json.dumps(node, sort_keys=True, indent=4))
