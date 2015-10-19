DEBUG = False

BROKER_URL = 'amqp://guest@localhost'

RECORD_HTTP_TRANSACTIONS = False

CELERY_EAGER_PROPAGATES_EXCEPTIONS = True

RAW_PROCESSING = ['cassandra', 'postgres']
NORMALIZED_PROCESSING = ['elasticsearch', 'cassandra', 'postgres']
RESPONSE_PROCESSOR = 'cassandra'
CANONICAL_PROCESSOR = 'postgres'

SENTRY_DSN = None

USE_FLUENTD = False

CASSANDRA_URI = ['127.0.0.1']
CASSANDRA_KEYSPACE = 'scrapi'

ELASTIC_URI = 'localhost:9200'
ELASTIC_TIMEOUT = 10
ELASTIC_INDEX = 'share'

PLOS_API_KEY = 'fakekey'
HARVARD_DATAVERSE_API_KEY = 'anotherfakekey'
SPRINGER_KEY = 'thisistotallyfakealso'
VIVO_ACCESS = {
    'url': 'http://dev.vivo.ufl.edu/',
    'query_endpoint': 'http://dev.vivo.ufl.edu/api/sparqlQuery',
    'username': 'fake_user@ufl.edu',
    'password': 'fakepassword'
}

disabled = ['stepic']

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
