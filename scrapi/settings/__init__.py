import os
import yaml

from celery.schedules import crontab

from . import defaults, local


# Programmatically generate celery beat schedule
def load_manifests():
    manifests = {}
    for path in os.listdir('TODO'):
        with open(path) as manifest_file:
            loaded = yaml.load(manifest_file)

        schedule = crontab(day_of_week=loaded['days'],
            hour=loaded['hour'], minute=loaded['minute']),

        manifests[path] = {
            'task': 'TODO',
            'schedule': schedule,
            'args': ''
        }
    return manifests

CELERYBEAT_SCHEDULE = load_manifests()

CELERYBEAT_SCHEDULE['check_archive'] = {
    'task': 'worker_manager.celerytasks.check_archive',
    'schedule': crontab(day_of_month='1', hour='23', minute='59'),
}

CELERYBEAT_SCHEDULE['tar archive'] = {
    'task': 'worker_manager.celerytasks.tar_archive',
    'schedule': crontab(hour="3", minute="00")
}
