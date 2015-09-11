DEBUG = False

ELASTIC_TIMEOUT = 10
ELASTIC_INDEX = 'share_v2'
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

disabled = []

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
SPRINGER_KEY = None

TEST_RECORD_MODE = 'new_episodes'
