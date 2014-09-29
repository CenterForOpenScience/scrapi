import os
import platform

from invoke import run, task

from scrapi import settings


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
    for consumer, manifest in settings.MANIFESTS.items():
        directory = 'scrapi/consumers/{}'.format(manifest['directory'].lower())
        if not os.path.isdir(directory):
            run('git clone -b master {git-url} {moddir}'.format(moddir=directory, **manifest))
        elif update:
            run('cd {} && git pull origin master'.format(directory))

        if os.path.isfile('{}/requirements.txt'.format(directory)):
            run('pip install -r {}/requirements.txt'.format(directory))


@task
def celery_beat():
    run('celery -A scrapi.tasks beat --loglevel info')


@task
def celery_worker():
    run('celery -A scrapi.tasks worker --loglevel info')


@task
def consume(consumer_name, async=False):
    settings.CELERY_ALWAYS_EAGER = not async
    from scrapi.tasks import run_consumer
    run_consumer(consumer_name)


@task
def run_consumers(async=False):
    pass  # TODO


@task
def check_archive(consumer=None, reprocess=False, async=False):
    settings.CELERY_ALWAYS_EAGER = not async

    if consumer:
        from scrapi.tasks import check_archive as check
        check.delay(consumer, reprocess)
    else:
        from scrapi.tasks import check_archives
        check_archives.delay(reprocess)
