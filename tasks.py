import logging
import platform
from datetime import date, timedelta

import urllib
from invoke import run, task
from elasticsearch import helpers

import scrapi.harvesters  # noqa
from dateutil.parser import parse
from scrapi import linter
from scrapi import registry
from scrapi import settings

from scrapi.processing.elasticsearch import es

logger = logging.getLogger()


@task
def reindex(src, dest):
    helpers.reindex(es, src, dest)
    es.indices.delete(src)


@task
def alias(alias, index):
    es.indices.delete_alias(index=alias, name='_all', ignore=404)
    es.indices.put_alias(alias, index)


@task
def renormalize(sources=None):
    from scripts.renormalize import renormalize
    renormalize(sources.split(',') if sources else [])


@task
def migrate(migration, kwargs_string, dry=True, async=False):
    ''' Task to run a migration. kwargs_string is a string passed to the
    invoke task that is then parsed into an optional set of keyword
    arguments. This is a way to get around passing a variable number
    of kwargs to the invoke task.

    An example of usage would be:
        inv migrate rename 'source: mit, target:mit2' --no-dry --no-async
    '''
    from scrapi import migrations
    from scrapi.tasks import migrate

    kwargs = {
        key.strip(): val.strip() for key, val in map(lambda x: x.split(':'), kwargs_string.split(','))
    }

    kwargs['dry'] = dry
    kwargs['async'] = async

    migrate_func = migrations.__dict__[migration]

    migrate(migrate_func, **kwargs)


@task
def delete(source):
    from scripts.delete import delete_by_source
    delete_by_source(source)


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
        cmd += ' --cov-report term-missing --cov-config .coveragerc --cov scrapi'

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
def worker():
    from scrapi.tasks import app
    app.worker_main(['worker', '--loglevel', 'info'])


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
        es.index(
            'share_providers',
            harvester.short_name,
            body={
                'favicon': 'data:image/png;base64,' + urllib.quote(open("img/favicons/{}_favicon.ico".format(harvester.short_name), "rb").read().encode('base64')),
                'short_name': harvester.short_name,
                'long_name': harvester.long_name,
                'url': harvester.url
            },
            id=harvester.short_name,
            refresh=True
        )
    print(es.count('share_providers', body={'query': {'match_all': {}}})['count'])
