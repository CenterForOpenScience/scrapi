"""
    Configuration file for celerybeat/worker.

    Dynamically adds harvesters from all manifest files in harvesterManifests
    to the celerybeat schedule.
"""
import os
import json
import logging

from fluent import sender

from celery.schedules import crontab

from raven import Client
from raven.contrib.celery import register_signal

from scrapi.settings.defaults import *
from scrapi.settings.local import *


logging.basicConfig(level=logging.INFO)
logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(logging.WARNING)


MANIFEST_DIR = os.path.join(os.path.dirname(__file__), 'harvesterManifests')


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
    for harvester_name, manifest in MANIFESTS.items():
        cron = crontab(day_of_week=manifest['days'],
                       hour=manifest['hour'], minute=manifest['minute'])

        schedule['run_{}'.format(harvester_name)] = {
            'task': 'scrapi.tasks.run_harvester',
            'schedule': cron,
            'args': [harvester_name]
        }
    return schedule


if USE_FLUENTD:
    sender.setup(**FLUENTD_ARGS)


if SENTRY_DSN:
    client = Client(SENTRY_DSN)
    register_signal(client)


MANIFESTS = load_manifests()

CELERY_ENABLE_UTC = True
CELERY_RESULT_BACKEND = None
CELERY_TASK_SERIALIZER = 'pickle'
CELERY_ACCEPT_CONTENT = ['pickle']
CELERY_RESULT_SERIALIZER = 'pickle'
CELERY_IMPORTS = ('scrapi.tasks', )


# Celery Beat Stuff
CELERYBEAT_SCHEDULE = create_schedule()

# CELERYBEAT_SCHEDULE['update pubsubhubbub'] = {
#     'task': 'scrapi.tasks.update_pubsubhubbub',
#     'schedule': crontab(minute='*/5')
# }
