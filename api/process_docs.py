# This file will be used to process raw and preprocessed-JSON documents from the scrapers
import os
import json
import datetime
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


BASE_DIR = os.path.abspath(os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    os.pardir,
))


def process_raw(doc, source, doc_id, filetype):
    """
        Takes a document (in the form of text, and saves it to disk
        with the specified name and the designated filetype in the
        specified source directory
    """
    timestamp = datetime.datetime.now()
    directory = 'archive/' + str(source).replace('/', '') + '/' + str(doc_id).replace('/', '') + '/' + str(timestamp) + '/'
    filepath = BASE_DIR + directory + "raw" + '.' + str(filetype)
    print filepath

    dir_path = BASE_DIR
    for dir in directory.split("/"):
        dir_path += dir + "/"
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

    with open(filepath, 'w') as f:
        f.write(str(doc))

    return "Success"


def process(doc, timestamp):
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
    print(timestamp)
    directory = 'archive/' + doc['source'].replace('/', '') + '/' + doc['id'].replace('/', '') + '/' + str(timestamp) + '/'
    filepath = BASE_DIR + directory + "parsed.json"

    dir_path = BASE_DIR
    for dir in directory.split("/"):
        dir_path += dir + "/"
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

    with open(filepath, 'w') as f:
        f.write(json.dumps(doc, sort_keys=True, indent=4))
    node = {}
    node['title'] = doc['title']
    node['contributors'] = doc['contributors']

    properties = doc['properties']
    for property in properties.keys():
        if property in ['description', 'tags', 'system_tags', 'wiki_pages_current']:
            node[property] = properties[property]

    return json.dumps(node, sort_keys=True, indent=4)
