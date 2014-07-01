from invoke import run, task


@task
def server():
    run("python website/main.py")
