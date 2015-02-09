import logging

from cqlengine.connection import LOG

from scrapi import settings
from scrapi.util.storage import store

from migration_tasks import process_one_to_cassandra


logger = logging.getLogger(__name__)
LOG.setLevel(logging.WARN)


def main():
    for consumer_name in settings.MANIFESTS.keys():
        consumer = settings.MANIFESTS[consumer_name]
        for raw_path in store.iter_raws(consumer_name, include_normalized=True):
            process_one_to_cassandra.delay(consumer_name, consumer, raw_path)


if __name__ == '__main__':
    main()
