DEBUG = False

RAISE_IN_TRANSFORMER = True

BROKER_URL = 'amqp://guest@localhost'

RECORD_HTTP_TRANSACTIONS = False

disabled = []
RAW_PROCESSING = []
NORMALIZED_PROCESSING = []
RESPONSE_PROCESSOR = ''
CANONICAL_PROCESSOR = ''

FRONTEND_KEYS = None

SENTRY_DSN = None

USE_FLUENTD = False
FLUENTD_ARGS = {
    'tag': 'app.scrapi',
}

DAYS_BACK = 2

# Retrying Celery tasks
CELERY_RETRY_DELAY = 30
CELERY_MAX_RETRIES = 5

VIVO_ACCESS = {
    'url': '',
    'query_endpoint': '',
    'username': '',
    'password': ''
}

CELERY_EAGER_PROPAGATES_EXCEPTIONS = True

ELASTIC_TIMEOUT = 10
ELASTIC_INDEX = 'share_v2'
ELASTIC_URI = 'localhost:9200'
ELASTIC_INST_INDEX = 'institutions'

CASSANDRA_URI = ['127.0.0.1']
CASSANDRA_KEYSPACE = 'scrapi'

FRONTEND_KEYS = [
    "uris",
    "contributors",
    "providerUpdatedDateTime",
    "description",
    "title",
    "freeToRead",
    "languages",
    "licenses",
    "publisher",
    "subjects",
    "tags",
    "sponsorships",
    "otherProperties",
    "shareProperties"
]

PLOS_API_KEY = None
HARVARD_DATAVERSE_API_KEY = None
SPRINGER_API_KEY = None

SHARE_REG_URL = None
