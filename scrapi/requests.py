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
from requests.structures import CaseInsensitiveDict

from scrapi import events
from scrapi import database
from scrapi import settings

logging.getLogger('cqlengine.cql').setLevel(logging.WARN)


@database.register_model
class HarvesterResponse(cqlengine.Model):
    __table_name__ = 'responses'

    method = columns.Text(primary_key=True)
    url = columns.Text(primary_key=True, required=True)

    # Raw request data
    ok = columns.Boolean()
    content = columns.Bytes()
    encoding = columns.Text()
    headers_str = columns.Text()
    status_code = columns.Integer()
    time_made = columns.DateTime(default=datetime.now)

    @property
    def json(self):
        return json.loads(self.content)

    @property
    def headers(self):
        return CaseInsensitiveDict(json.loads(self.headers_str))


def record_or_load_response(method, url, **kwargs):
    try:
        resp = HarvesterResponse.get(url=url, method=method)
        if resp.ok:
            return resp
    except HarvesterResponse.DoesNotExist:
        resp = None

    response = requests.request(method, url, **kwargs)

    if not response.ok:
        events.log_to_sentry('Got non-ok response code.', url=url, method=method)

    if resp:
        return resp.update(
            ok=response.ok,
            content=response.content,
            encoding=response.encoding,
            status_code=response.status_code,
            headers_str=json.dumps(dict(response.headers))
        ).save()
    else:
        return HarvesterResponse(
            url=url,
            method=method,
            ok=response.ok,
            content=response.content,
            encoding=response.encoding,
            status_code=response.status_code,
            headers_str=json.dumps(dict(response.headers))
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
