import os
import json
import shutil
import platform
import subprocess

from invoke import run, task

from scrapi import linter
from scrapi import settings
from scrapi.util import import_consumer


@task
def server():
    run("python website/main.py")


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
        print("Your system is not recognized, you will have to start elasticsearch manually")


@task
def test():
    """
    Runs all tests in the 'tests/' directory
    """
    # Allow selecting specific submodule
    args = " --verbosity={0} -s {1}".format(2, 'tests/')
    # Use pty so the process buffers "correctly"
    run('nosetests --rednose' + args, pty=True)


@task
def requirements():
    run('pip install -r requirements.txt')


@task
def migrate_search():
    run('python website/migrate_search.py')


@task
def install_consumers(update=False):
    if update:
        run('cd scrapi/settings/consumerManifests && git reset HEAD --hard && git pull origin master')
        settings.MANIFESTS = settings.load_manifests()

    for consumer, manifest in settings.MANIFESTS.items():
        directory = 'scrapi/consumers/{}'.format(manifest['shortName'])

        if not os.path.isdir(directory):
            run('git clone -b master {url} {moddir}'.format(moddir=directory, **manifest))
        elif update:
            run('cd {} && git pull origin master'.format(directory))

        manifest_file = 'scrapi/settings/consumerManifests/{}.json'.format(consumer)

        with open(manifest_file) as f:
            loaded = json.load(f)

        loaded['version'] = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=directory).strip()

        with open(manifest_file, 'w') as f:
            json.dump(loaded, f, indent=4, sort_keys=True)

        if os.path.isfile('{}/requirements.txt'.format(directory)):
            run('pip install -r {}/requirements.txt'.format(directory))


@task
def clean_consumers():
    run('cd scrapi/settings/consumerManifests && git reset HEAD --hard && git pull origin master')

    for listing in os.listdir('scrapi/consumers'):
        path = os.path.join('scrapi', 'consumers', listing)
        if os.path.isdir(path):
            print 'Removing {}...'.format(path)
            shutil.rmtree(path)


@task
def beat():
    run('celery -A scrapi.tasks beat --loglevel info')


@task
def worker():
    run('celery -A scrapi.tasks worker --loglevel info')


@task
def consumer(consumer_name, async=False, days=1):
    settings.CELERY_ALWAYS_EAGER = not async
    from scrapi.tasks import run_consumer

    if not settings.MANIFESTS.get(consumer_name):
        print 'No such consumers {}'.format(consumer_name)

    run_consumer.delay(consumer_name, days_back=days)


@task
def consumers(async=False, days=1):
    settings.CELERY_ALWAYS_EAGER = not async
    from scrapi.tasks import run_consumer

    for consumer_name in settings.MANIFESTS.keys():
        run_consumer.delay(consumer_name, days_back=days)


@task
def check_archive(consumer=None, reprocess=False, async=False):
    settings.CELERY_ALWAYS_EAGER = not async

    if consumer:
        from scrapi.tasks import check_archive as check
        check.delay(consumer, reprocess)
    else:
        from scrapi.tasks import check_archives
        check_archives.delay(reprocess)


@task
def lint_all():
    for name in settings.MANIFESTS.keys():
        lint(name)


@task
def lint(name):
    manifest = settings.MANIFESTS[name]
    consumer = import_consumer(name)
    try:
        linter.lint(consumer.consume, consumer.normalize)
    except Exception as e:
        print 'Consumer {} raise the following exception'.format(manifest['longName'])
        print e
