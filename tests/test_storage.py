import utils

from mock import mock_open, patch

from scrapi.processing.storage import StorageProcessor
from scrapi.linter.document import RawDocument, NormalizedDocument

test_db = StorageProcessor()

RAW = RawDocument(utils.RAW_DOC)
NORMALIZED = NormalizedDocument(utils.NORMALIZED_DOC)


@patch('scrapi.processing.storage.os')
def test_process_normalized(mock_os):
    mock_os.path.exists.return_value = False
    filename = 'archive/{}/{}/normalized.json'.format(RAW['source'], RAW['docID'])
    m = mock_open()
    with patch('scrapi.processing.storage.open', m, create=True):
        test_db.process_normalized(RAW, NORMALIZED)

    m.assert_called_once_with(filename, 'w')


@patch('scrapi.processing.storage.os')
def test_process_raw(mock_os):
    mock_os.path.exists.return_value = False
    filename = 'archive/{}/{}/raw.{}'.format(RAW['source'], RAW['docID'], RAW['filetype'])
    m = mock_open()
    with patch('scrapi.processing.storage.open', m, create=True):
        test_db.process_raw(RAW)

    m.assert_called_once_with(filename, 'w')
