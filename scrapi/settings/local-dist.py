DEBUG = False

ELASTIC_TIMEOUT = 10
ELASTIC_INDEX = 'share'
ELASTIC_URI = 'localhost:9200'

BROKER_URL = 'amqp://guest@localhost'

CELERY_EAGER_PROPAGATES_EXCEPTIONS = True

RECORD_HTTP_TRANSACTIONS = False

NORMALIZED_PROCESSING = []
RAW_PROCESSING = []

SENTRY_DSN = None

USE_FLUENTD = False
FLUENTD_ARGS = {
    'tag': 'app.scrapi'
}


CASSANDRA_URI = ['127.0.0.1']
CASSANDRA_KEYSPACE = 'scrapi'

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
