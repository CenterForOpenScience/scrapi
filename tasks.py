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
