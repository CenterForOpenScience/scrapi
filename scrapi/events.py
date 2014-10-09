from fluent import event

from scrapi import settings

# Events
PROCESSING = 'processing'
CONSUMER_RUN = 'runConsumer'
CHECK_ARCHIVE = 'checkArchive'
NORMALIZATION = 'normalization'

# statuses
FAILED = 'failed'
SKIPPED = 'skipped'
CREATED = 'created'
STARTED = 'started'
COMPLETED = 'completed'


# Ues _index here as to not clutter the namespace for kwargs
def dispatch(_event, status, _index=None, **kwargs):
    if settings.USE_FLUENTD:
        evnt = {
            'event': _event,
            'status': status
        }

        evnt.update(kwargs)

        if _index:
            _event = '{}.{}'.format(_event, _index)

        event.Event(_event, evnt)
