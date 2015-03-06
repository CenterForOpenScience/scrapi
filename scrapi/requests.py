from __future__ import absolute_import

import json
import logging
import functools
from datetime import datetime

import requests
import cqlengine
from cqlengine import columns
from cqlengine import management
from cassandra.cluster import NoHostAvailable

from scrapi import settings


logger = logging.getLogger(__name__)


try:
    cqlengine.connection.setup(settings.CASSANDRA_URI, settings.CASSANDRA_KEYSPACE)
    management.create_keyspace(settings.CASSANDRA_KEYSPACE, replication_factor=1, strategy_class='SimpleStrategy')
except NoHostAvailable:
    logger.error('Could not connect to Cassandra, expect errors.')
    if settings.RECORD_HTTP_TRANSACTIONS:
        raise


class HarvesterResponse(cqlengine.Model):
    __table_name__ = 'responses'
    __keyspace__ = settings.CASSANDRA_KEYSPACE

    method = columns.Text(primary_key=True)
    url = columns.Text(primary_key=True, required=True, index=True)

    # Raw request data
    content = columns.Bytes()
    headers_str = columns.Text()
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
            content=response.content
        ).save()


def request(method, url, **kwargs):
    if settings.RECORD_HTTP_TRANSACTIONS:
        return record_or_load_response(method, url)
    return requests.request(method, url, **kwargs)


# This has to be done after HarvesterResponse definition
management.sync_table(HarvesterResponse)

get = functools.partial(request, 'get')
put = functools.partial(request, 'put')
post = functools.partial(request, 'post')
delete = functools.partial(request, 'delete')
