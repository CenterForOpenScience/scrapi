import os
import sys
import json
sys.path.insert(1, os.path.abspath(os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    os.pardir,
)))
from worker_manager.schedule import load_config
from website import search
from pyelasticsearch.exceptions import ElasticHttpNotFoundError, ElasticHttpError


def migrate():
    try:
        search.delete_all('scrapi')
    except ElasticHttpNotFoundError:
        pass
    all_yamls = {}
    for filename in os.listdir('worker_manager/manifests/'):
        yaml_dict = load_config('worker_manager/manifests/' + filename)
        all_yamls[yaml_dict['directory']] = yaml_dict['url']

    for dirname, dirnames, filenames in os.walk('archive/'):
        if os.path.isfile(dirname + '/parsed.json'):
            with open(dirname + '/parsed.json') as f:
                doc = json.load(f)
                try:
                    search.update('scrapi', doc, dirname.split('/')[1], dirname.split('/')[2])
                except ElasticHttpError:
                    pass

if __name__ == '__main__':
    migrate()
