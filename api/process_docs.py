# This file will be used to process raw and preprocessed-JSON documents from the scrapers
import os
import json
import datetime
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import logging
sys.path.insert(1, os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    os.pardir,
))
from api.collision_detection import collision_detector

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

BASE_DIR = os.path.abspath(os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    os.pardir,
))


def process_raw(doc, source, doc_id, filetype, consumer_version):
    """
        Takes a text document and saves it to disk with the specified name and the designated filetype
        in the specified source directory. It also saves a manifest file in the directory, containing
        the git hash for the version of the consumer it was produced with.
    """
    timestamp = datetime.datetime.now()
    directory = '/archive/' + str(source).replace('/', '') + '/' + str(doc_id).replace('/', '') + '/' + str(timestamp) + '/'
    filepath = BASE_DIR + directory + "raw" + '.' + str(filetype)

    dir_path = BASE_DIR
    for dir in directory.split("/"):
        dir_path += dir + "/"
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

    with open(filepath, 'w') as f:
        try:
            f.write(str(doc))
        except IOError as e:
            logger.log(e)
            return None
    with open(dir_path + 'manifest.json', 'w') as f:
        info = {}
        info['consumer_version'] = consumer_version.strip()
        info['source'] = source.strip()
        info['timestamp'] = str(timestamp)
        f.write(json.dumps(info))

    return timestamp


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
    if None in [doc.get('title'), doc.get('contributors'), doc.get('id'), doc.get('source')]:
        logger.error(": Document sent for processing did not match schema")
        return None

    # Collision detection
    conflict = collision_detector.detect(doc)
    if conflict:
        logger.warn("Document with id {0} and timestamp {1} from source {2} already found in database".format(doc['id'], timestamp, doc['source']))

    directory = '/archive/' + doc['source'].replace('/', '') + '/' + str(doc['id']).replace('/', '') + '/' + str(timestamp) + '/'
    filepath = BASE_DIR + directory + "parsed.json"

    dir_path = BASE_DIR
    for dir in directory.split("/"):
        dir_path += dir + "/"
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

    doc['timestamp'] = str(timestamp)

    with open(filepath, 'w') as f:
        try:
            f.write(json.dumps(doc, sort_keys=True, indent=4))
        except IOError as e:
            logger.error(e)
            return None
    return doc
