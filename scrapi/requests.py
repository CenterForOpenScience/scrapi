from __future__ import absolute_import

import json
import logging
import functools
from datetime import datetime

import requests
import cqlengine
from cqlengine import columns

from scrapi import database  # noqa
from scrapi import settings


logger = logging.getLogger(__name__)


class HarvesterResponse(cqlengine.Model):
    __table_name__ = 'responses'
    __keyspace__ = settings.CASSANDRA_KEYSPACE

    method = columns.Text(primary_key=True)
    url = columns.Text(primary_key=True, required=True, index=True)

    # Raw request data
    content = columns.Bytes()
    headers_str = columns.Text()
    status_code = columns.Integer()
    time_made = columns.DateTime(default=datetime.now)

    @property
    def json(self):
        return json.loads(self.content)

    @property
    def headers(self):
        # TODO: make case insensitive multidict
        return json.loads(self.headers_str)


def record_or_load_response(method, url, **kwargs):
    try:
        return HarvesterResponse.get(url=url, method=method)
    except HarvesterResponse.DoesNotExist:
        response = requests.request(method, url, **kwargs)
        return HarvesterResponse(
            url=url,
            method=method,
            content=response.content,
            status_code=response.status_code,
            headers_str=json.dumps(response.headers)
        ).save()


def request(method, url, **kwargs):
    if settings.RECORD_HTTP_TRANSACTIONS:
        return record_or_load_response(method, url, **kwargs)
    return requests.request(method, url, **kwargs)


get = functools.partial(request, 'get')
put = functools.partial(request, 'put')
post = functools.partial(request, 'post')
delete = functools.partial(request, 'delete')
