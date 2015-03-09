DEBUG = False

BROKER_URL = 'amqp://guest@localhost'

STORAGE_METHOD = 'disk'
ARCHIVE_DIRECTORY = 'archive/'
RECORD_DIRECTORY = 'records'

RECORD_HTTP_TRANSACTIONS = False

CELERY_EAGER_PROPAGATES_EXCEPTIONS = True

RAW_PROCESSING = ['storage', 'cassandra']
NORMALIZED_PROCESSING = ['storage', 'elasticsearch', 'cassandra']

SENTRY_DSN = None

USE_FLUENTD = False

CASSANDRA_URI = ['127.0.0.1']
CASSANDRA_KEYSPACE = 'scrapi'

ELASTIC_URI = 'localhost:9200'
ELASTIC_TIMEOUT = 10
ELASTIC_INDEX = 'share'

FRONTEND_KEYS = [
    u'description',
    u'contributors',
    u'tags',
    u'raw',
    u'title',
    u'id',
    u'source',
    u'dateUpdated'
]
