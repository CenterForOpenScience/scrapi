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

CELERY_IMPORTS = ('celerytasks',)

# Programmatically generate celery beat schedule
SCHED = {}
for manifest in os.listdir('manifests/'):
    filepath = 'manifests/' + manifest
    with open(filepath) as f:
        info = yaml.load(f)
    SCHED['run ' + manifest] = {
        'task': 'celerytasks.run_scraper',
        'schedule': crontab(day_of_week='mon-fri', hour=info['hour'], minute=info['minute']),
        'args': [filepath],
    }

SCHED['add'] = {
    'task': 'celerytasks.add',
    'schedule': timedelta(seconds=30),
    'args': (16, 16)
}

CELERYBEAT_SCHEDULE = SCHED
