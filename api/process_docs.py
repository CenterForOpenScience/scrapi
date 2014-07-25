"""
    This file is used to process raw and normalized documents retrieved by consumers.

    Raw files are written to disk, and normalized documents are analyzed for collisions
    and then also written to disk.
"""
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


def process_raw(attributes, consumer_version):
    """
        Takes a RawFile object and saves it to disk with the specified name and the designated filetype
        in the specified source directory. It also saves a manifest file in the directory, containing
        the git hash for the version of the consumer it was produced with.
    """
    timestamp = datetime.datetime.now()
    directory = '/archive/' + str(attributes.get('source')).replace('/', '') \
        + '/' + str(attributes.get("doc_id")).replace('/', '') \
        + '/' + str(timestamp) + '/'

    filepath = BASE_DIR + directory + "raw" + '.' + str(attributes.get('filetype'))

    dir_path = BASE_DIR
    for dir in directory.split("/"):
        dir_path += dir + "/"
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

    with open(filepath, 'w') as f:
        try:
            f.write(str(attributes.get("doc")))
        except IOError as e:
            logger.log(e)
            return None
    with open(dir_path + 'manifest.json', 'w') as f:
        info = {}
        info['consumer_version'] = consumer_version.strip()
        info['source'] = attributes.get("source").strip()
        info['timestamp'] = str(timestamp)
        f.write(json.dumps(info))

    return timestamp


def process(attributes, timestamp):
    """
        Takes a NormalizedFile object and extracts the information necessary
        to add the document to the search engine. The attributes field of the
        NormalizedFile object has the following structure:
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
    if None in [attributes.get('title'), attributes.get('contributors'), attributes.get('id'), attributes.get('source')]:
        logger.error(": Document sent for processing did not match schema")
        return None

    # Collision detection
    conflict = collision_detector.detect(attributes.attributes)
    if conflict:
        logger.warn("Document with id {0} and timestamp {1} from source {2} already found in database"
                    .format(attributes.get('id'), timestamp, attributes.get('source')))

    directory = '/archive/' + attributes.get('source').replace('/', '') + '/' + str(attributes.get('id')).replace('/', '') + '/' + str(timestamp) + '/'
    filepath = BASE_DIR + directory + "normalized.json"

    dir_path = BASE_DIR
    for dir in directory.split("/"):
        dir_path += dir + "/"
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

    with open(filepath, 'w') as f:
        try:
            f.write(json.dumps(attributes.attributes, sort_keys=True, indent=4))
        except IOError as e:
            logger.error(e)
            return None
    return attributes
