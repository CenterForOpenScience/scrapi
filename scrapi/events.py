from fluent import event

from scrapi import settings

# Events
CONSUMER_RUN = 'runConsumer'
NORMALIZATION = 'normalization'

# statuses
CREATED = 'created'
STARTED = 'started'
COMPLETED = 'completed'
FAILED = 'failed'


def dispatch(_event, status, **kwargs):
    if settings.USE_FLUENTD:
        evnt = {
            'event': _event,
            'status': status
        }

        evnt.update(kwargs)
        event.Event(None, evnt)
