import six
from datetime import date
import json
from datetime import timedelta

from scrapi import requests
from scrapi import settings
from scrapi.base import BaseHarvester
from scrapi.base import HarvesterMeta
from scrapi.linter.document import RawDocument, NormalizedDocument


def gen_harvesters():
    sources = requests.get('{}/sources'.format(settings.SHARE_REG_URL), force=True).json()
    return {
        source['short_name']: gen_harvester(**source)
        for source in sources
    }


def gen_harvester(short_name=None, long_name=None, url=None):
    return type(
        '{}Harvester'.format(short_name.lower().capitalize()),
        (PushApiHarvester, ),
        dict(short_name=short_name, long_name=long_name, url=url)
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
                'source': self.short_name,
                'docID': record['shareProperties']['docID'],
                'filetype': 'json'
            }) for record in self.get_records()
        ]

    def get_records(self, start_date, end_date):
        response = requests.get(
            '{}/established'.format('settings.SHARE_REG_URL'),
            params={'from': start_date.isoformat(), 'to': end_date.isoformat()}
        ).json()
        for record in response['results']:
            yield record

        while response.get('next'):
            response = requests.get(response['next'])
            for record in response['results']:
                yield record

    def normalize(self, raw):
        return NormalizedDocument(json.loads(raw['doc']))

    def run_at(self):
        return {
            'minute': '*/15'
        }


harvesters = gen_harvesters()
