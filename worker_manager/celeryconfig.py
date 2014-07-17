from celery.schedules import crontab
from datetime import timedelta
import os
import yaml

BROKER_URL = 'amqp://guest@localhost'
CELERY_RESULT_BACKEND = 'amqp://guest@localhost'

CELERY_TASK_SERIALIZER = 'pickle'
CELERY_RESULT_SERIALIZER = 'pickle'
CELERY_ACCEPT_CONTENT = ['pickle']
CELERY_ENABLE_UTC = True
CELERY_TIMEZONE = 'UTC'

CELERY_IMPORTS = ('worker_manager.celerytasks',)

# Programmatically generate celery beat schedule
SCHED = {}
for manifest in os.listdir('worker_manager/manifests/'):
    filepath = 'worker_manager/manifests/' + manifest
    with open(filepath) as f:
        info = yaml.load(f)
    SCHED['run ' + manifest] = {
        'task': 'worker_manager.celerytasks.run_consumer',
        'schedule': crontab(day_of_week=info['days'], hour=info['hour'], minute=info['minute']),
        'args': [filepath],
    }

SCHED['request normalization of recent documents'] = {
    'task': 'worker_manager.celerytasks.request_normalized',
    'schedule': crontab(minute='*/1')
}

SCHED['check_archive'] = {
    'task': 'worker_manager.celerytasks.check_archive',
    'schedule': crontab(day_of_month='1', hour='23', minute='59'),
}

SCHED['add'] = {
    'task': 'worker_manager.celerytasks.heartbeat',
    'schedule': timedelta(seconds=30),
    'args': (16, 16)
}

CELERYBEAT_SCHEDULE = SCHED
