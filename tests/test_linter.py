import mock
import pytest

from scrapi import linter
from .utils import RAW_DOC
from .utils import NORMALIZED_DOC


class TestLinterUtils(object):

    def test_picklable(self):
        linter.is_picklable({'': 'str'})

    def test_picklable_raises(self):
        with pytest.raises(TypeError):
            linter.is_picklable({'': lambda x: x})

    def test_serializeable(self):
        linter.is_serializable({'': 'str'})

    def test_serializeable_raises(self):
        with pytest.raises(TypeError):
            linter.is_picklable({'': lambda x: x})


class TestLint(object):

    def test_non_list(self):
        with pytest.raises(TypeError) as e:
            linter.lint(lambda: 'str', None)
        assert e.value.message.endswith('does not return type list')

    def test_empty_list(self):
        with pytest.raises(ValueError) as e:
            linter.lint(lambda: [], None)
        assert e.value.message.endswith('returned an empty list')

    def test_must_be_rawdoc(self):
        with pytest.raises(TypeError) as e:
            linter.lint(lambda: ['Nonraw'], None)
        assert e.value.message.endswith('returned a list containing a non-RawDocument item')

    def test_calls_normalize(self):
        mock_normalize = mock.Mock(return_value=linter.NormalizedDocument(NORMALIZED_DOC))
        raw_doc = linter.RawDocument(RAW_DOC)
        linter.lint(lambda: [raw_doc], mock_normalize)

        mock_normalize.assert_called_once_with(raw_doc)

    def test_throws_on_non_normalized(self):
        mock_normalize = mock.Mock(return_value='foo')
        raw_doc = linter.RawDocument(RAW_DOC)

        with pytest.raises(TypeError) as e:
            linter.lint(lambda: [raw_doc], mock_normalize)

        assert e.value.message.endswith(' does not return type NormalizedDocument')

    def test_non_matching_service_ids(self):
        mock_normalize = mock.Mock(return_value=linter.NormalizedDocument(NORMALIZED_DOC))
        raw_doc = linter.RawDocument(RAW_DOC)
        raw_doc['docID'] = u'Nope'

        with pytest.raises(ValueError) as e:
            linter.lint(lambda: [raw_doc], mock_normalize)

        assert e.value.message == 'Service ID someID does not match Nope'
        raw_doc['docID'] = u'someID'

    def test_works(self):
        mock_normalize = mock.Mock(return_value=linter.NormalizedDocument(NORMALIZED_DOC))
        raw_doc = linter.RawDocument(RAW_DOC)
        assert 'Linting passed with No Errors' == linter.lint(lambda: [raw_doc], mock_normalize)

