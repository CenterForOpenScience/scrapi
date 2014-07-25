import normalize
import consume
from datetime import datetime

result_list = consume.consume()

try:
    first_result = result_list[0][0]
    normed_file = normalize.normalize(first_result, datetime.now())

except IndexError: 
    print "There are no more new files..."
    normed_file = []

def test_first_dict():
    try:
        assert isinstance(normed_file, dict)
    except NameError:
        print "no new files..."
        assert normed_file == []

def test_contributors_exists():    
    try:
        assert normed_file['doc']['contributors']
    except NameError:
        print "no new files..."
        assert normed_file == []

def test_title():
    try:
        assert isinstance(normed_file['doc']['title'], basestring)
    except NameError:
        print "no new files..."
        assert normed_file == []

def test_contributors_is_string():
    try:    
        assert isinstance(normed_file['doc']['contributors'], basestring)
    except NameError:
        print "no new files..."
        assert normed_file == []

def test_abstract_is_string():
    try:
        assert isinstance(normed_file['doc']['properties']['abstract'], basestring)
    except NameError:
        print "no new files..."
        assert normed_file == []

def test_id_is_string():
    try:
        assert isinstance(normed_file['doc']['id'], basestring)
    except NameError:
        print "no new files..."
        assert normed_file == []

def test_source():
    try:
        assert normed_file['doc']['source'] == 'ClinicalTrials.gov'
    except NameError:
        print "no new files..."
        assert normed_file == []

def test_timestamp():
    try:
        timestamp = normed_file['doc']['timestamp']
        timestamp = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f')
        assert isinstance(timestamp, datetime)
    except NameError:
        print "no new files..."
        assert normed_file == []



