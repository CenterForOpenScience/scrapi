# This file will be used to process raw and preprocessed-JSON documents from the scrapers
import os

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
    f = open(filepath,'w')
    f.write(doc)
    f.close()
    #with open(filepath, 'w+') as f:
    #    f.write(doc)

def process(doc):
    """
        Takes a JSON document and extracts the information necessary
        to make an OSF project, then creates that OSF project through
        the API (does not exist yet)
    """
    raise NotImplementedError
