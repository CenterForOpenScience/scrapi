import os
import base64
import logging
import platform
from datetime import date, timedelta

from invoke import run, task
# from elasticsearch import helpers
# from dateutil.parser import parse
# from six.moves.urllib import parse as urllib_parse

# import scrapi.harvesters  # noqa
# from scrapi import linter
# from scrapi import registry
# from scrapi import settings


logger = logging.getLogger()

WHEELHOUSE_PATH = os.environ.get('WHEELHOUSE')


@task
def reindex(src, dest):
    ''' Reindexes documents from index {src} to index {dest}'''
    from elasticsearch import helpers
    from scrapi.processing.elasticsearch import DatabaseManager
    dm = DatabaseManager()
    dm.setup()

    helpers.reindex(dm.es, src, dest)
    dm.es.indices.delete(src)


@task
def alias(alias, index):
    ''' Creates an Elasticsearch index alias '''
    from scrapi.processing.elasticsearch import DatabaseManager
    dm = DatabaseManager()
    dm.setup()
    dm.es.indices.delete_alias(index=alias, name='_all', ignore=404)
    dm.es.indices.put_alias(alias, index)


@task
def migrate(migration, sources=None, kwargs_string=None, dry=True, async=False, group_size=1000):
    ''' Task to run a migration.

    :param migration: The migration function to run. This is passed in
    as a string then interpreted as a function by the invoke task.
    :type migration: str

    :param kwargs_string: parsed into an optional set of keyword
    arguments, so that the invoke migrate task can accept a variable
    number of arguments for each migration.
    The kwargs_string should be in the following format:
        'key:value, key2:value2'
    ...with the keys and values seperated by colons, and each kwarg seperated
    by commas.
    :type kwarg_string: str

    An example of usage renaming mit to mit 2 as a real run would be:
        inv migrate rename -s mit -k 'target:mit2' --no-dry
    An example of calling renormalize on two sources as an async dry run:
        inv migrate renormalize -s 'mit,asu' -a
    '''
    kwargs_string = kwargs_string or ':'
    sources = sources or ''

    from scrapi import migrations
    from scrapi.tasks import migrate

    kwargs = {}
    for key, val in map(lambda x: x.split(':'), kwargs_string.split(',')):
        key, val = key.strip(), val.strip()
        if key not in kwargs.keys():
            kwargs[key] = val
        elif isinstance(kwargs[key], list):
            kwargs[key].append(val)
        else:
            kwargs[key] = [kwargs[key], val]

    kwargs['dry'] = dry
    kwargs['async'] = async
    kwargs['group_size'] = group_size
    kwargs['sources'] = list(map(lambda x: x.strip(), sources.split(',')))

    if kwargs['sources'] == ['']:
        kwargs.pop('sources')

    migrate_func = migrations.__dict__[migration]

    migrate(migrate_func, **kwargs)


@task
def restart_search():
    ''' Restarts Elasticsearch '''
    run("curl -XPOST 'http://localhost:9200/_shutdown'")
    if platform.linux_distribution()[0] == 'Ubuntu':
        run("sudo service elasticsearch restart")
    elif platform.system() == 'Darwin':  # Mac OSX
        run('elasticsearch')


@task
def elasticsearch():
    '''Start a local elasticsearch server

    NOTE: Requires that elasticsearch is installed. See README for instructions
    '''
    if platform.linux_distribution()[0] == 'Ubuntu':
        run("sudo service elasticsearch restart")
    elif platform.system() == 'Darwin':  # Mac OSX
        run('elasticsearch')
    else:
        print(
            "Your system is not recognized, you will have to start elasticsearch manually")


@task
def test(cov=True, doctests=True, verbose=False, debug=False, pdb=False):
    """
    Runs all tests in the 'tests/' directory, and all doctests in the scrAPI directory
    """
    cmd = 'py.test scrapi tests'
    if doctests:
        cmd += ' --doctest-modules'
    if verbose:
        cmd += ' -v'
    if debug:
        cmd += ' -s'
    if cov:
        cmd += ' --cov-report term-missing --cov-config .coveragerc --cov scrapi --cov api'
    if pdb:
        cmd += ' --pdb'

    run(cmd, pty=True)


@task
def wheelhouse(develop=False):
    ''' Sets up the wheelhouse for requirements '''
    req_file = 'dev-requirements.txt' if develop else 'requirements.txt'
    cmd = 'pip wheel --find-links={} -r {} --wheel-dir={}'.format(WHEELHOUSE_PATH, req_file, WHEELHOUSE_PATH)
    run(cmd, pty=True)


@task
def requirements(develop=False, upgrade=False):
    ''' Installs all Python dependencies '''
    req_file = 'dev-requirements.txt' if develop else 'requirements.txt'
    cmd = 'pip install -r {}'.format(req_file)

    if upgrade:
        cmd += ' --upgrade'
    if WHEELHOUSE_PATH:
        cmd += ' --no-index --find-links={}'.format(WHEELHOUSE_PATH)
    run(cmd, pty=True)


@task
def beat(setup=True):
    ''' Runs the Celery Beat service '''
    from scrapi import registry
    from scrapi.tasks import app
    # Set up the provider map for elasticsearch
    if setup:
        provider_map(delete=True)

    app.conf['CELERYBEAT_SCHEDULE'] = registry.beat_schedule
    app.Beat().run()


@task
def worker(loglevel='INFO', hostname='%h', autoreload=False):
    ''' Runs the Celery worker service '''
    from scrapi.tasks import app
    command = ['worker']
    if loglevel:
        command.extend(['--loglevel', loglevel])
    if hostname:
        command.extend(['--hostname', hostname])
    if autoreload:
        command.extend(['--autoreload'])
    app.worker_main(command)


@task
def harvester(harvester_name, async=False, start=None, end=None):
    ''' Runs a specific harvester '''
    from scrapi import settings
    settings.CELERY_ALWAYS_EAGER = not async
    from scrapi import registry
    from scrapi.tasks import run_harvester
    from dateutil.parser import parse

    if not registry.get(harvester_name):
        raise ValueError('No such harvesters {}'.format(harvester_name))

    end = parse(end).date() if end else date.today()
    start = parse(start).date() if start else end - timedelta(settings.DAYS_BACK)

    run_harvester.delay(harvester_name, start_date=start, end_date=end)


@task
def harvesters(async=False, start=None, end=None):
    ''' Runs all harvesters '''
    from scrapi import settings
    settings.CELERY_ALWAYS_EAGER = not async
    from scrapi import registry
    from scrapi.tasks import run_harvester
    from dateutil.parser import parse

    start = parse(start).date() if start else date.today() - timedelta(settings.DAYS_BACK)
    end = parse(end).date() if end else date.today()

    exceptions = []
    for harvester_name in registry.keys():
        try:
            run_harvester.delay(harvester_name, start_date=start, end_date=end)
        except Exception as e:
            logger.exception(e)
            exceptions.append(e)

    logger.info("\n\nNumber of exceptions: {}".format(len(exceptions)))
    for exception in exceptions:
        logger.exception(e)


@task
def provider_map(delete=False):
    ''' Adds favicons and metadata for harvesters to Elasticsearch '''
    from six.moves.urllib import parse as urllib_parse
    from scrapi import registry
    from scrapi.base.helpers import null_on_error
    from scrapi.processing.elasticsearch import DatabaseManager
    dm = DatabaseManager()
    dm.setup()
    es = dm.es
    if delete:
        es.indices.delete(index='share_providers', ignore=[404])
    from scrapi.harvesters.push_api import gen_harvesters
    gen_harvesters()

    for harvester_name, harvester in registry.items():
        if not null_on_error(es.get, log=False)(index='share_providers', doc_type=harvester_name, id=harvester_name):
            with open("img/favicons/{}_favicon.ico".format(harvester.short_name), "rb") as f:
                favicon = urllib_parse.quote(base64.encodestring(f.read()))

            es.index(
                'share_providers',
                harvester.short_name,
                body={
                    'favicon': 'data:image/png;base64,' + favicon,
                    'short_name': harvester.short_name,
                    'long_name': harvester.long_name,
                    'url': harvester.url
                },
                id=harvester.short_name,
                refresh=True
            )
    print(es.count('share_providers', body={'query': {'match_all': {}}})['count'])


@task
def apiserver():
    ''' Runs the Django apiserver '''
    os.system('python manage.py runserver')


@task
def apidb():
    ''' Runs the Django migrations '''
    os.system('python manage.py migrate')


@task
def reset_all():
    ''' WARNING: Nukes all databases and sets them back up. Only run in development '''
    import sys
    from scrapi import settings

    if sys.version[0] == "3":
        raw_input = input

    if raw_input('Are you sure? y/N ') != 'y':
        return
    os.system('psql -c "DROP DATABASE scrapi;" template1')
    os.system('psql -c "CREATE DATABASE scrapi;" template1')
    os.system('python manage.py migrate')

    os.system("curl -XDELETE '{}/share*'".format(settings.ELASTIC_URI))
    os.system("invoke alias share share_v2")
    os.system("invoke provider_map")


@task
def institutions(grid_file='institutions/grid_2015_11_05.json', ipeds_file='institutions/hd2014.csv'):
    ''' Loads the institution data into Elasticsearch '''
    grid(grid_file)
    ipeds(ipeds_file)


@task
def remove_institutions(force=False):
    ''' Removes the institutions index from Elasticsearch '''
    import six
    if not force:
        resp = six.moves.input('You are about to delete the institutions index. Are you sure? (y, n)\n')
        if resp not in ('y', 'Y', 'Yes', 'yes'):
            print('Remove institutions stopped.')
            return
    from institutions.institutions import remove
    remove()


def grid(filepath):
    from institutions import institutions, grid
    institutions.setup()
    grid.populate(filepath)


def ipeds(filepath):
    from institutions import institutions, ipeds
    institutions.setup()
    ipeds.populate(filepath)


@task(default=True)
def usage():
    ''' Lists the available commands '''
    run('invoke --list')
