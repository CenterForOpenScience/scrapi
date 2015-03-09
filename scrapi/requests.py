"""A wrapper around requests that records all requests made with it.
    Supports get, put, post, delete and request
    all calls return an instance of HarvesterResponse
"""
from __future__ import absolute_import

import json
import logging
import functools
from datetime import datetime

import requests
import cqlengine
from cqlengine import columns

from scrapi import database
from scrapi import settings

logger = logging.getLogger(__name__)


@database.register_model
class HarvesterResponse(cqlengine.Model):
    __table_name__ = 'responses'
    __keyspace__ = settings.CASSANDRA_KEYSPACE

    method = columns.Text(primary_key=True)
    url = columns.Text(primary_key=True, required=True)

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
    """Make a recorded request or get a record matching method and url
    :param str method: Get, Put, Post, or Delete
    :param str url: Where to make the request to
    :param dict kwargs: Addition keywords to pass to requests
    """
    if settings.RECORD_HTTP_TRANSACTIONS:
        return record_or_load_response(method, url, **kwargs)
    return requests.request(method, url, **kwargs)


get = functools.partial(request, 'get')
put = functools.partial(request, 'put')
post = functools.partial(request, 'post')
delete = functools.partial(request, 'delete')
