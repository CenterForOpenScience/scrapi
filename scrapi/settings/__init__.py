"""
    Configuration file for celerybeat/worker.

    Dynamically adds consumers from all manifest files in worker_manager/manifests/
    to the celerybeat schedule. Also adds a heartbeat function to the schedule,
    which adds every 30 seconds, and a monthly task to normalize all non-normalized
    documents.
"""
import os
import json
import logging

from celery.schedules import crontab

from raven import Client
from raven.contrib.celery import register_signal

from scrapi.settings.local import *
from scrapi.settings.defaults import *


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(logging.WARNING)

MANIFEST_DIR = os.path.join(os.path.dirname(__file__), 'consumerManifests')

if SENTRY_DNS:
    client = Client(SENTRY_DNS)
    register_signal(client)


# Programmatically generate celery beat schedule
def load_manifests():
    manifests = {}
    for path in os.listdir(MANIFEST_DIR):
        if '.json' not in path:
            continue

        with open(os.path.join(MANIFEST_DIR, path)) as manifest_file:
            loaded = json.load(manifest_file)

        manifests[loaded['shortName']] = loaded

    return manifests


def create_schedule():
    schedule = {}
    for consumer_name, manifest in MANIFESTS.items():
        cron = crontab(day_of_week=manifest['days'],
            hour=manifest['hour'], minute=manifest['minute'])

        schedule['run_{}'.format(consumer_name)] = {
            'task': 'scrapi.tasks.run_consumer',
            'schedule': cron,
            'args': [consumer_name]
        }
    return schedule


MANIFESTS = load_manifests()

CELERYBEAT_SCHEDULE = create_schedule()

CELERY_ALWAYS_EAGER = False

CELERY_TASK_SERIALIZER = 'pickle'
CELERY_RESULT_SERIALIZER = 'pickle'
CELERY_ACCEPT_CONTENT = ['pickle']
CELERY_ENABLE_UTC = True
CELERY_TIMEZONE = 'UTC'

CELERY_IMPORTS = ('scrapi.tasks',)

CELERYBEAT_SCHEDULE['check_archive'] = {
    'task': 'scrapi.tasks.check_archive',
    'schedule': crontab(day_of_month='1', hour='23', minute='59'),
}

CELERYBEAT_SCHEDULE['tar archive'] = {
    'task': 'scrapi.tasks.tar_archive',
    'schedule': crontab(hour="3", minute="00")
}
