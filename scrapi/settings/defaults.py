BROKER_URL = 'amqp://guest@localhost'
CELERY_RESULT_BACKEND = 'amqp://guest@localhost'
CELERY_EAGER_PROPAGATES_EXCEPTIONS = True

STORAGE_METHOD = 'disk'
ARCHIVE_DIRECTORY = 'archive/'
RECORD_DIRECTORY = 'records'

STORE_HTTP_TRANSACTIONS = False

NORMALIZED_PROCESSING = ['storage']
RAW_PROCESSING = ['storage']

SENTRY_DNS = None

FLUENTD_ARGS = None

# OUTPUT SETTINGS
OSF_ENABLED = False

PROTOCOL = 'http'
VERIFY_SSL = True
OSF_PREFIX = 'localhost:5000'

APP_ID = 'some id'

API_KEY_LABEL = 'some label'
API_KEY = 'some api key'

OSF_AUTH = (API_KEY_LABEL, API_KEY)
