import logging
import inspect
from functools import wraps
from contextlib import contextmanager

from fluent import event

from scrapi import settings


logger = logging.getLogger(__name__)

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


class Skipped(Exception):
    pass


def serialize_fluent_data(data):
    if isinstance(data, dict):
        return {
            key: serialize_fluent_data(val)
            for key, val in data.items()
        }
    elif isinstance(data, list):
        return [
            serialize_fluent_data(item)
            for item in data
        ]
    else:
        return repr(data)


# Ues _index here as to not clutter the namespace for kwargs
def dispatch(_event, status, _index=None, **kwargs):
    if not settings.USE_FLUENTD:
        logger.warning('Dispatched called but USE_FLUENTD is False')
        return
    evnt = {
        'event': _event,
        'status': status
    }

    evnt.update(serialize_fluent_data(kwargs))

    if _index:
        _event = '{}.{}'.format(_event, _index)

    logger.info('[{}][{}]{!r}'.format(_event, status, kwargs))
    event.Event(_event, evnt)


def logged(event, index=None):
    def _logged(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            context = extract_context(func, *args, **kwargs)
            dispatch(event, STARTED, _index=index, **context)
            try:
                res = func(*args, **kwargs)
            except Skip as e:
                dispatch(event, SKIPPED, _index=index, reason=e.message, **context)
                return None
            except Exception as e:
                dispatch(event, FAILED, _index=index, exception=e, **context)
                raise
            else:
                dispatch(event, COMPLETED, _index=index, **context)
            return res
        return wrapped
    return _logged


@contextmanager
def logged_failure(event, index=None, reraise=True):
    try:
        yield
    except Exception as e:
        arg_info = inspect.getargvalues(inspect.stack()[2][0])
        context = {
            k: v
            for k, v in arg_info.locals.items()
            if k in arg_info.args or arg_info.keywords
        }
        dispatch(event, FAILED, exception=repr(e), **context)
        if reraise:
            raise


def extract_context(func, *args, **kwargs):
    full_args = list(args) + kwargs.values()
    names = list(func.func_code.co_varnames) + kwargs.keys()
    return {
        key: val
        for key, val
        in zip(names, full_args)
    }


def log_task_created(event, context):
    dispatch(event, CREATED, **context)


def creates_task(event):
    def _creates_task(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            res = func(*args, **kwargs)
            dispatch(event, CREATED, **extract_context(func, *args, **kwargs))
            return res
        return wrapped
    return _creates_task
