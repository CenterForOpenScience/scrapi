DEBUG = False

BROKER_URL = 'amqp://guest@localhost'

STORAGE_METHOD = 'disk'
ARCHIVE_DIRECTORY = 'archive/'
RECORD_DIRECTORY = 'records'

CELERY_EAGER_PROPAGATES_EXCEPTIONS = True

STORE_HTTP_TRANSACTIONS = False

NORMALIZED_PROCESSING = ['storage']
RAW_PROCESSING = ['storage']

SENTRY_DNS = None

USE_FLUENTD = False
FLUENTD_ARGS = ('scrapi',)

# OUTPUT SETTINGS
OSF_ENABLED = False

PROTOCOL = 'http'
VERIFY_SSL = True
OSF_PREFIX = 'localhost:5000'

APP_ID = 'some id'

API_KEY_LABEL = 'some label'
API_KEY = 'some api key'
