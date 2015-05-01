import logging
import platform

import urllib
from invoke import run, task
from elasticsearch import helpers

import scrapi.harvesters  # noqa
from scrapi import linter
from scrapi import registry
from scrapi import settings

from scrapi.processing.elasticsearch import es

logger = logging.getLogger()


@task
def server():
    run("python server.py")


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
def rename(source, target, dry=True):
    from scripts.rename import rename
    rename(source, target, dry)


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
def test(cov=True, verbose=False):
    """
    Runs all tests in the 'tests/' directory
    """
    cmd = 'py.test tests'
    if verbose:
        cmd += ' -v'
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
def harvester(harvester_name, async=False, days=1):
    settings.CELERY_ALWAYS_EAGER = not async
    from scrapi.tasks import run_harvester

    if not registry.get(harvester_name):
        raise ValueError('No such harvesters {}'.format(harvester_name))

    run_harvester.delay(harvester_name, days_back=days)


@task
def harvesters(async=False, days=1):
    settings.CELERY_ALWAYS_EAGER = not async
    from scrapi.tasks import run_harvester

    exceptions = []
    for harvester_name in registry.keys():
        try:
            run_harvester.delay(harvester_name, days_back=days)
        except Exception as e:
            logger.exception(e)
            exceptions.append(e)

    logger.info("\n\nNumber of exceptions: {}".format(len(exceptions)))
    for exception in exceptions:
        logger.exception(e)


@task
def check_archive(harvester=None, reprocess=False, async=False, days=None):
    settings.CELERY_ALWAYS_EAGER = not async

    if harvester:
        from scrapi.tasks import check_archive as check
        check.delay(harvester, reprocess, days_back=int(days))
    else:
        from scrapi.tasks import check_archives
        check_archives.delay(reprocess, days_back=int(days))


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
def provider_map():
    from scrapi.processing.elasticsearch import es
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
