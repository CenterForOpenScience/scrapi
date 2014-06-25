# This file will be used to process raw and preprocessed-JSON documents from the scrapers


def process_raw(doc, source, id, filetype):
    """
        Takes a document (in the form of text, and saves it to disk
        with the specified name and the designated filetype in the
        specified source directory
    """
    filepath = 'raw' + '/' + str(source) + str(id) + '.' + str(filetype)
    with open(filepath, 'w') as f:
        f.write(doc)


def process(doc):
    """
        Takes a JSON document and extracts the information necessary
        to make an OSF project, then creates that OSF project through
        the API (does not exist yet)
    """
    raise NotImplementedError
