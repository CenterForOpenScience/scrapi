from invoke import run, task


@task
def server():
    run("python website/main.py")


@task
def schedule():
    run("python worker_manager/schedule.py")
