import six
import json
import logging
from datetime import date
from datetime import timedelta

import requests

from scrapi import settings
from scrapi.base import BaseHarvester, HarvesterMeta
from scrapi.linter.document import RawDocument, NormalizedDocument

logger = logging.getLogger(__name__)


def gen_harvesters():
    try:
        return {
            source['shortname']: gen_harvester(**source)
            for source in get_sources()
        }
    except Exception as e:
        logger.warn('Could not generate harvesters from push api')
        if settings.DEBUG:
            logger.exception(e)


def get_sources():
    response = requests.get('{}/provider_list'.format(settings.SHARE_REG_URL)).json()
    while True:
        for source in response['results']:
            yield source
        if response['next']:
            response = requests.get(response['next']).json()
        else:
            break


def gen_harvester(shortname=None, longname=None, url=None, favicon_dataurl=None, **kwargs):
    assert shortname and longname and url and favicon_dataurl
    logger.info('Generating harvester {}'.format(shortname))

    from scrapi.processing.elasticsearch import DatabaseManager
    dm = DatabaseManager()
    dm.setup()
    es = dm.es

    es.index(
        'share_providers',
        shortname,
        body={
            'favicon': favicon_dataurl,
            'short_name': shortname,
            'long_name': longname,
            'url': url
        },
        id=shortname,
        refresh=True
    )
    return type(
        '{}Harvester'.format(shortname.lower().capitalize()),
        (PushApiHarvester, ),
        dict(short_name=shortname, long_name=longname, url=url)
    )


@six.add_metaclass(HarvesterMeta)
class PushApiHarvester(BaseHarvester):
    file_format = 'json'

    def harvest(self, start_date=None, end_date=None):
        start_date = start_date or date.today() - timedelta(settings.DAYS_BACK)
        end_date = end_date or date.today()

        return [
            RawDocument({
                'doc': json.dumps(record),
                'source': record['source'],
                'docID': record['docID'],
                'filetype': 'json'
            }) for record in self.get_records(start_date, end_date)
        ]

    def get_records(self, start_date, end_date):
        response = requests.get(
            '{}/established'.format(settings.SHARE_REG_URL),
            params={
                'from': start_date.isoformat(),
                'to': end_date.isoformat(),
                'source': self.short_name
            }
        )
        response = response.json()
        for record in response['results']:
            yield record

        while response.get('next'):
            response = requests.get(response['next']).json()
            for record in response['results']:
                yield record

    def normalize(self, raw):
        document = json.loads(raw['doc'])['jsonData']
        # This is a workaround for the push API did not have proper email validation
        for contributor in document['contributors']:
            if contributor['email'] == '':
                del contributor['email']
        return NormalizedDocument(document)

    @property
    def run_at(self):
        return {
            'minute': '*/15'
        }


harvesters = gen_harvesters()
