from scrapi import util


class TestScrapiUtils(object):
    def test_copy_to_unicode(self):
        converted = util.copy_to_unicode('test')

        assert converted == u'test'
        assert isinstance(converted, unicode)
