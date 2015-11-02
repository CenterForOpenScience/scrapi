import six

from scrapi import requests
from scrapi import settings
from scrapi.base import BaseHarvester
from scrapi.base import HarvesterMeta
from scrapi.linter.document import RawDocument


def gen_harvesters():
    # sources = requests.get('{}/sources'.format(settings.SHARE_REG_URL), force=True).json()
    sources = [{
        'short_name': 'totally',
        'long_name': 'real',
        'url': 'http://talk.com'
    }, {
        'short_name': 'wow',
        'long_name': 'so',
        'url': 'http://harvester.com'
    }]
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
        pass

    def normalize(self, raw):
        pass


harvesters = gen_harvesters()
