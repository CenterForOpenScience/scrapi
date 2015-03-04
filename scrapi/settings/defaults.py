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
FLUENTD_ARGS = {
    'tag': 'app.scrapi'
}

# OUTPUT SETTINGS
OSF_ENABLED = False

VERIFY_SSL = True
OSF_PREFIX = 'http://localhost:5000'

APP_ID = 'some id'

API_KEY_LABEL = 'some label'
API_KEY = 'some api key'

SCRAPI_URL = 'http://173.255.232.219'

ES_SEARCH_MAPPING = {
    "properties": {
        "id": {
            "properties": {
                "doi": {
                    "type": "multi_field",
                    "index": "not_analyzed",
                    "fields": {
                        "analyzed": {
                            "type": "string",
                            "index": "analyzed"
                        }
                    }
                },
                "url": {
                    "type": "multi_field",
                    "index": "not_analyzed",
                    "fields": {
                        "analyzed": {
                            "type": "string",
                            "index": "analyzed"
                        }
                    }
                },
                "serviceID": {
                    "type": "multi_field",
                    "index": "not_analyzed",
                    "fields": {
                        "analyzed": {
                            "type": "string",
                            "index": "analyzed"
                        }
                    }
                }
            }
        }
    }
}
