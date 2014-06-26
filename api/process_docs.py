# This file will be used to process raw and preprocessed-JSON documents from the scrapers
import os
import json

def process_raw(doc, source, id, filetype):
    """
        Takes a document (in the form of text, and saves it to disk
        with the specified name and the designated filetype in the
        specified source directory
    """
    directory = '/home/faye/cos/scrapi/api/raw/' + str(source) +'/'
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
    """

    directory = '/home/faye/cos/scrapi/api/json/' + doc['source'] +'/'
    filepath = directory + doc['id'].replace('/','%2F') + '.json'
    if not os.path.exists(directory):
        os.makedirs(directory)
    with open(filepath, 'w') as f:
        f.write(json.dumps(doc,sort_keys=True, indent=4))
