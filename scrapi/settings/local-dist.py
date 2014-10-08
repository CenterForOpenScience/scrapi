DEBUG = True

BROKER_URL = 'amqp://guest@localhost'
CELERY_RESULT_BACKEND = 'amqp://guest@localhost'

STORAGE_METHOD = 'disk'
ARCHIVE_DIRECTORY = 'archive/'

STORE_HTTP_TRANSACTIONS = False

NORMALIZED_PROCESSING = ['storage']
RAW_PROCESSING = ['storage']

USE_FLUENTD = False

OSF_PREFIX = 'localhost:5000'

APP_ID = 'someid'

API_KEY_LABEL = 'scrapi'
API_KEY = 'keeeeeeeeeeeeeeeeeeeeeeeeeeeeeey'
