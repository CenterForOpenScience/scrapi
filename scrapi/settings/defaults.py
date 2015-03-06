DEBUG = False

BROKER_URL = 'amqp://guest@localhost'

STORAGE_METHOD = 'disk'
ARCHIVE_DIRECTORY = 'archive/'
RECORD_DIRECTORY = 'records'

CELERY_EAGER_PROPAGATES_EXCEPTIONS = True

RECORD_HTTP_TRANSACTIONS = False

NORMALIZED_PROCESSING = ['storage']
RAW_PROCESSING = ['storage']

SENTRY_DSN = None

USE_FLUENTD = False
FLUENTD_ARGS = {
    'tag': 'app.scrapi'
}

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
