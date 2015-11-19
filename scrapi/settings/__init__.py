import logging

from raven import Client
from fluent import sender
from raven.contrib.celery import register_signal

from scrapi.settings.defaults import *

logging.basicConfig(level=logging.INFO)
logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

try:
    from scrapi.settings.local import *
except ImportError as error:
    logger.warn("No scrapi local.py settings file found. Try running $ cp scrapi/settings/local-dist.py scrapi/settings/local.py. Defaulting to scrapi/settings/defaults.py")

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
CELERY_IMPORTS = ('scrapi.tasks', 'scrapi.migrations')
