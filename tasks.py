import os
import base64
import logging
import platform
from datetime import date, timedelta

from invoke import run, task
from elasticsearch import helpers
from dateutil.parser import parse
from six.moves.urllib import parse as urllib_parse

import scrapi.harvesters  # noqa
from scrapi import linter
from scrapi import registry
from scrapi import settings


logger = logging.getLogger()


@task
def reindex(src, dest):
    from scrapi.processing.elasticsearch import es
    helpers.reindex(es, src, dest)
    es.indices.delete(src)


@task
def alias(alias, index):
    from scrapi.processing.elasticsearch import es
    es.indices.delete_alias(index=alias, name='_all', ignore=404)
    es.indices.put_alias(alias, index)


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
    kwargs['sources'] = map(lambda x: x.strip(), sources.split(','))

    if kwargs['sources'] == ['']:
        kwargs.pop('sources')

    migrate_func = migrations.__dict__[migration]

    migrate(migrate_func, **kwargs)


@task
def migrate_to_source_partition(dry=True, async=False):
    from scrapi.tasks import migrate_to_source_partition
    migrate_to_source_partition(dry=dry, async=async)


@task
def reset_search():
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
def test(cov=True, verbose=False, debug=False):
    """
    Runs all tests in the 'tests/' directory
    """
    cmd = 'py.test tests'
    if verbose:
        cmd += ' -v'
    if debug:
        cmd += ' -s'
    if cov:
        cmd += ' --cov-report term-missing --cov-config .coveragerc --cov scrapi --cov api'

    run(cmd, pty=True)


@task
def requirements():
    run('pip install -r requirements.txt')


@task
def beat():
    from scrapi.tasks import app
    app.conf['CELERYBEAT_SCHEDULE'] = registry.beat_schedule
    app.Beat().run()


@task
def worker(loglevel='INFO', hostname='%h'):
    from scrapi.tasks import app
    command = ['worker']
    if loglevel:
        command.extend(['--loglevel', loglevel])
    if hostname:
        command.extend(['--hostname', hostname])
    app.worker_main(command)


@task
def harvester(harvester_name, async=False, start=None, end=None):
    settings.CELERY_ALWAYS_EAGER = not async
    from scrapi.tasks import run_harvester

    if not registry.get(harvester_name):
        raise ValueError('No such harvesters {}'.format(harvester_name))

    start = parse(start).date() if start else date.today() - timedelta(settings.DAYS_BACK)
    end = parse(end).date() if end else date.today()

    run_harvester.delay(harvester_name, start_date=start, end_date=end)


@task
def harvesters(async=False, start=None, end=None):
    settings.CELERY_ALWAYS_EAGER = not async
    from scrapi.tasks import run_harvester

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
def lint_all():
    for name in registry.keys():
        lint(name)


@task
def lint(name):
    harvester = registry[name]
    try:
        linter.lint(harvester.harvest, harvester.normalize)
    except Exception as e:
        print('Harvester {} raise the following exception'.format(harvester.short_name))
        print(e)


@task
def provider_map(delete=False):
    from scrapi.processing.elasticsearch import es
    if delete:
        es.indices.delete(index='share_providers', ignore=[404])

    for harvester_name, harvester in registry.items():
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
    os.system('python manage.py runserver')


@task
def apidb():
    os.system('python manage.py migrate')
