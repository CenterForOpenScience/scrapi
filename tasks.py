from invoke import run, task
import os
import yaml


@task
def server():
    run("python website/main.py")


@task
def schedule():
    run("python worker_manager/schedule.py")


@task
def reset_search():
    run("curl -XPOST 'http://localhost:9200/_shutdown'")
    import platform
    if platform.linux_distribution()[0] == 'Ubuntu':
        run("sudo service elasticsearch restart")
    elif platform.system() == 'Darwin':  # Mac OSX
        run('elasticsearch')


@task
def elasticsearch():
    '''Start a local elasticsearch server

    NOTE: Requires that elasticsearch is installed. See README for instructions
    '''
    import platform
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
def install_consumers(virtualenv=''):
    virtualenv = virtualenv + 'bin/' if virtualenv else ''
    for filename in os.listdir('worker_manager/manifests/'):
        with open('worker_manager/manifests/' + filename) as f:
            info = yaml.load(f)
        if not os.path.isdir('worker_manager/consumers/{0}'.format(info['directory'])):
            run('git clone -b master {0} worker_manager/consumers/{1}'.format(info['git-url'], info['directory']))
            run('{1}pip install -r worker_manager/consumers/{0}/requirements.txt'.format(info['directory'], virtualenv))


@task
def celery_beat(virtualenv=''):
    virtualenv = virtualenv + 'bin/' if virtualenv else ''
    run('{0}celery -A worker_manager.celerytasks beat --loglevel info'.format(virtualenv))


@task
def celery_worker(virtualenv=''):
    virtualenv = virtualenv + 'bin/' if virtualenv else ''
    run('{0}celery -A worker_manager.celerytasks worker --loglevel info'.format(virtualenv))


@task
def run_consumers(manifest=None):
    from worker_manager.celerytasks import run_consumers, run_consumer
    if manifest:
        run_consumer('worker_manager/manifests/{}.yml'.format(manifest))
    else:
        run_consumers()


@task
def check_archive(directory='', reprocess=False):
    from worker_manager.celerytasks import check_archive
    check_archive(directory, reprocess)
