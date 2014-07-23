import sys
import xmltodict
from bs4 import BeautifulSoup
from datetime import datetime
reload(sys)
sys.setdefaultencoding('utf-8')

def getcontributors(result):
    thelist = result['root']['metadata']['oai_dc:dc']['dc:contributor']['item']
    thelistcomprehended = [ item['#text'] for item in thelist ]
    return thelistcomprehended

def getabstract(result):
    abstract = result['root']['metadata']['oai_dc:dc']['key'][7]['#text']
    return abstract

def getid(result):
    theid = result['root']['header']['identifier']['#text']
    return theid

def normalize(result, timestamp):
    result2 = xmltodict.parse(result)
    #import pdb; pdb.set_trace()
    payload = {
        "doc": {
            'title': result2['root']['metadata']['oai_dc:dc']['key'][5]['#text'],
            'contributors': [result2['root']['metadata']['oai_dc:dc']['key'][6]['#text']] + getcontributors(result2),
            'properties': {
                'abstract': getabstract(result2)
            },
            'meta': {},
            'id': getid(result2),
            'source': "VTechWorks",
            'timestamp': str(timestamp)
        }
    }
    return payload

# these tests assume that a file named 'output.xml' was created, using the first item of the tuple generated at
# the end of consume.py; that file generation line is still in there, but you'll have to uncomment it to test

# def test_normalize_makes_dictionary():
#     with open('output.xml','r') as f:
#         theresult = f.read()
#     assert isinstance(normalize(theresult, datetime.now()), dict)

# def test_get_contributors():
#     with open('output.xml','r') as f:
#         theresult = f.read()
#         result2 = xmltodict.parse(theresult)
#     assert isinstance(getcontributors(result2), list)
#     assert getcontributors(result2)[0] == 'Mechanical Engineering'

# def test_get_abstract():
#     with open('output.xml','r') as f:
#         theresult = f.read()
#         result2 = xmltodict.parse(theresult)
#     #print(type(getabstract(result2)))
#     assert isinstance(getabstract(result2), (str,unicode))

# def test_get_id():
#     with open('output.xml','r') as f:
#         theresult = f.read()
#         result2 = xmltodict.parse(theresult)
#     assert isinstance(getid(result2), (str,unicode))
#     assert getid(result2) == 'oai:vtechworks.lib.vt.edu:10919/49545' 
