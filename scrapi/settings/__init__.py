import logging

from raven import Client
from fluent import sender
from raven.contrib.celery import register_signal

from scrapi.settings.defaults import *
from scrapi.settings.local import *


logging.basicConfig(level=logging.INFO)
logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(logging.WARNING)


if USE_FLUENTD:
    sender.setup(**FLUENTD_ARGS)


if SENTRY_DSN:
    client = Client(SENTRY_DSN)
    register_signal(client)


CELERY_ENABLE_UTC = True
CELERY_RESULT_BACKEND = None
CELERY_TASK_SERIALIZER = 'pickle'
CELERY_ACCEPT_CONTENT = ['pickle']
CELERY_RESULT_SERIALIZER = 'pickle'
CELERY_IMPORTS = ('scrapi.tasks', )
