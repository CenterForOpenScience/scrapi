import logging

from cqlengine.connection import LOG

from scrapi import settings
from scrapi.util.storage import store

from migration_tasks import process_one


logger = logging.getLogger(__name__)
LOG.setLevel(logging.WARN)


def main():
    exceptions = []
    for harvester_name, harvester in settings.MANIFESTS.items():
        for raw_path in store.iter_raws(harvester_name, include_normalized=True):
            try:
                process_one.delay(harvester_name, harvester, raw_path)
            except Exception as e:
                logger.exception(e)
                exceptions.append(e)
    return exceptions

if __name__ == '__main__':
    exceptions = main()
    for e in exceptions:
        logger.exception(e)
