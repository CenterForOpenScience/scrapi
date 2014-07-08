from invoke import run, task


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
