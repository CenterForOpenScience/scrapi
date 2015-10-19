DEBUG = False

RAISE_IN_TRANSFORMER = True

BROKER_URL = 'amqp://guest@localhost'

CELERY_EAGER_PROPAGATES_EXCEPTIONS = True

RECORD_HTTP_TRANSACTIONS = False

disabled = []
RAW_PROCESSING = []
NORMALIZED_PROCESSING = []

FRONTEND_KEYS = None

SENTRY_DSN = None

USE_FLUENTD = False
FLUENTD_ARGS = {
    'tag': 'app.scrapi'
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
