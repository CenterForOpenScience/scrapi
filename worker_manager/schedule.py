from apscheduler.scheduler import Scheduler
import time
import os
import requests
import yaml

def load_config(config_file):
    config_file = file(config_file, 'r')
    info = yaml.load(config_file)
    return info

def main():
    info = load_config('manifests/plos-manifest.yml')

    day_of_week = info['days']
    hour = info['hour']
    minute = info['minute']

    config = {
        'apscheduler.jobstores.file.class': info['scheduler-config']['class'],
        'apscheduler.jobstores.file.path': info['scheduler-config']['path']
    }
    sched = Scheduler(config)

    sched.add_cron_job(plos, day_of_week=day_of_week, hour=hour, minute=minute)

    sched.start()

    print('Press Ctrl+{0} to exit'.format('C'))

    try:
        # This is here to simulate application activity (which keeps the main thread alive).
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        sched.shutdown()  # Not strictly necessary if daemonic mode is enabled but should be done if possibleisched.start()

def run_scraper():
    info = load_config('manifests/plos-manifest.yml')
    # does this need to be added to the yaml file? 
    r = requests.post("http://localhost:1338/consume")
    print r
    for dirname, dirnames, filenames in os.walk('../archive/'+ str(info['directory']) + '/'):
        # print path to all filenames.
        for filename in filenames:
            if 'raw' in filename:
                with open(os.path.join(dirname, filename), 'r') as f:
                    payload = {'doc': f.read()}
                    requests.post('http://localhost:1338/process', params=payload)  # TODO

if __name__ == '__main__':
    run_scraper()


# programatically add a new scraper - generalizing the plos function
# manifest file loading System
# how to store and load various scrapers
# on some schedule, send requests to scrapers to consume info and parse it
# when you add a scraper, the manifest.json file defines where you live and where you want us to run
# create a cron job based on that info in the manifest.json file

