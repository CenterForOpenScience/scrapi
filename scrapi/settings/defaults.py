"""
    Configuration file for celerybeat/worker.

    Dynamically adds consumers from all manifest files in worker_manager/manifests/
    to the celerybeat schedule. Also adds a heartbeat function to the schedule,
    which adds every 30 seconds, and a monthly task to normalize all non-normalized
    documents.
"""
BROKER_URL = 'amqp://guest@localhost'
# CELERY_RESULT_BACKEND = 'amqp://guest@localhost'

CELERY_TASK_SERIALIZER = 'pickle'
CELERY_RESULT_SERIALIZER = 'pickle'
CELERY_ACCEPT_CONTENT = ['pickle']
CELERY_ENABLE_UTC = True
CELERY_TIMEZONE = 'UTC'

CELERY_IMPORTS = ('worker_manager.celerytasks',)

#### OUTPUT SETTINGS ####
OSF_ENABLED = False

PROTOCOL = 'http'
VERIFY_SSL = True
OSF_PREFIX = 'localhost:5000'

APP_ID = 'some id'

API_KEY_LABEL = 'some label'
API_KEY = 'some api key'

OSF_AUTH = (API_KEY_LABEL, API_KEY)

NEW_RECORD_URL = '{PROTOCOL}://{API_KEY}@{OSF_PREFIX}/api/v1/{APP_ID}/reports/'
# Keep a pep8 line length
NEW_RECORD_URL = NEW_RECORD_URL.format(**locals())
