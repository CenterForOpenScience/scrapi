import logging
import platform

from invoke import run, task

from scrapi import linter
from scrapi import settings
from scrapi.util import import_harvester

logger = logging.getLogger()


@task
def server():
    run("python server.py")


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

    if not settings.MANIFESTS.get(harvester_name):
        raise ValueError('No such harvesters {}'.format(harvester_name))

    run_harvester.delay(harvester_name, days_back=days)


@task
def harvesters(async=False, days=1):
    settings.CELERY_ALWAYS_EAGER = not async
    from scrapi.tasks import run_harvester

    exceptions = []
    for harvester_name in settings.MANIFESTS.keys():
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
    for name in settings.MANIFESTS.keys():
        lint(name)


@task
def lint(name):
    manifest = settings.MANIFESTS[name]
    harvester = import_harvester(name)
    try:
        linter.lint(harvester.harvest, harvester.normalize)
    except Exception as e:
        print 'Harvester {} raise the following exception'.format(manifest['longName'])
        print e
