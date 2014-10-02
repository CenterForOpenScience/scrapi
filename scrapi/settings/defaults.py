BROKER_URL = 'amqp://guest@localhost'
CELERY_RESULT_BACKEND = 'amqp://guest@localhost'
CELERY_EAGER_PROPAGATES_EXCEPTIONS = True

STORAGE_METHOD = 'disk'
ARCHIVE_DIRECTORY = 'archive/'

NORMALIZED_PROCESSING = ['storage']
RAW_PROCESSING = ['storage']


# OUTPUT SETTINGS
OSF_ENABLED = False

PROTOCOL = 'http'
VERIFY_SSL = True
OSF_PREFIX = 'localhost:5000'

APP_ID = 'some id'

API_KEY_LABEL = 'some label'
API_KEY = 'some api key'
