from apscheduler.scheduler import Scheduler
import time
import sys
import os
sys.path.insert(1, '/home/fabian/cos/scrapi/')
import requests


def main():
    config = {
        'apscheduler.jobstores.file.class': 'apscheduler.jobstores.shelve_store:ShelveJobStore',
        'apscheduler.jobstores.file.path': '/tmp/dbfile'
    }
    sched = Scheduler(config)

    sched.add_cron_job(plos, day_of_week='mon-fri', hour=23, minute=59)
    sched.start()

    print('Press Ctrl+{0} to exit'.format('C'))

    try:
        # This is here to simulate application activity (which keeps the main thread alive).
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        sched.shutdown()  # Not strictly necessary if daemonic mode is enabled but should be done if possibleisched.start()


def plos():
    r = requests.post("http://localhost:1338/consume")
    print r
    for dirname, dirnames, filenames in os.walk('../archive/PLoS/'):
        # print path to all filenames.
        for filename in filenames:
            if 'raw' in filename:
                with open(os.path.join(dirname, filename), 'r') as f:
                    payload = {'doc': f.read()}
                    requests.post('http://localhost:1338/process', params=payload)  # TODO

def add_scraper():
    pass

if __name__ == '__main__':
    plos()


# programatically add a new scraper - generalizing the plos function
# manifest file loading System
# how to store and load various scrapers
# on some schedule, send requests to scrapers to consume info and parse it
# when you add a scraper, the manifest.json file defines where you live and where you want us to run
# create a cron job based on that info in the manifest.json file

