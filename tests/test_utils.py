import datetime
from dateutil.parser import parse

from scrapi import util


class TestScrapiUtils(object):

    def test_copy_to_unicode(self):
        converted = util.copy_to_unicode('test')

        assert converted == u'test'
        assert isinstance(converted, unicode)

    def test_timestamp(self):
        timestamp = util.timestamp()
        parsed = parse(timestamp)

        assert isinstance(parsed, datetime.datetime)

    def test_stamp_from_raw(self):
        raw_doc = {'doc': 'Macho Man Story', 'timestamps': {}}
        new_stamps = {'done': 'now'}

        stamped_raw = util.stamp_from_raw(raw_doc, **new_stamps)

        assert isinstance(stamped_raw, dict)
        assert set(stamped_raw.keys()) == set(['done', 'normalizeFinished'])
