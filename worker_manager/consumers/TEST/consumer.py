# test consumer

from scrapi_tools import lint
from scrapi_tools.document import RawDocument, NormalizedDocument

NAME = "TEST"

def consume():
    ''' return a list of info including a 'raw' file 
    '''
    records = [
    {'author': 'Darth Maul', 'title': 'Facepaint Tips Vol 2', 'id': 1, 'abstract':'A useful guide to facepaint.'}, 
    {'author': 'Cody Rhodes', 'title': 'Stardust: The Backstory', 'id': 2, 'abstract': 'Look up to the cosmos! It is the neverending void!'},
    {'author': 'Shawn Michaels', 'title': 'Ducks', 'id': 3, 'abstract':'All about ducks.'}]

    json_list = []
    for record in records:
        json_list.append(RawDocument({
                    'doc': record,
                    'source': NAME,
                    'doc_id': record['id'],
                    'filetype': 'json'
                }))

    return json_list


def normalize(raw_doc, timestamp):
    doc = raw_doc.get('doc')

    normalized_dict = {
            'title': doc['title'],
            'contributors': [doc['author']],
            'properties': {
                'abstract': doc['abstract']
            },
            'meta': {},
            'id': doc['id'],
            'source': NAME,
            'timestamp': str(timestamp)
    }

    return NormalizedDocument(normalized_dict)
        

if __name__ == '__main__':
    print(lint(consume, normalize))
