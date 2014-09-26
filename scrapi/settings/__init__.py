import os
import yaml
import logging

from celery.schedules import crontab

from . import defaults, local


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


MANIFEST_DIR = os.path.join(os.path.dirname(__file__), 'consumerManifests')


# Programmatically generate celery beat schedule
def load_manifests():
    manifests = {}
    for path in os.listdir(MANIFEST_DIR):
        if '.yml' not in path:
            continue

        with open(os.path.join(MANIFEST_DIR, path)) as manifest_file:
            loaded = yaml.load(manifest_file)
            manifests[path] = loaded

    return manifests


def create_schedule():
    schedule = {}
    for consumer_name, manifest in MANIFESTS.items():
        cron = crontab(day_of_week=manifest['days'],
            hour=manifest['hour'], minute=manifest['minute']),

        schedule['run_{}'.format(consumer_name)] = {
            'task': 'scrapi.tasks.run_consumer',
            'schedule': cron,
            'args': [consumer_name]
        }
    return schedule


MANIFESTS = load_manifests()

CELERYBEAT_SCHEDULE = create_schedule()

CELERY_ALWAYS_EAGER = False

CELERYBEAT_SCHEDULE['check_archive'] = {
    'task': 'worker_manager.celerytasks.check_archive',
    'schedule': crontab(day_of_month='1', hour='23', minute='59'),
}

CELERYBEAT_SCHEDULE['tar archive'] = {
    'task': 'worker_manager.celerytasks.tar_archive',
    'schedule': crontab(hour="3", minute="00")
}
