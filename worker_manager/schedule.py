from apscheduler.scheduler import Scheduler
import time
import os
import requests
import yaml
import logging 

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def load_config(config_file):
    with open(config_file) as f: 
        info = yaml.load(f)
    return info

def main(config_file):
    info = load_config(config_file)

    day_of_week = info['days']
    hour = info['hour']
    minute = info['minute']

    config = {
        'apscheduler.jobstores.file.class': info['scheduler-config']['class'],
        'apscheduler.jobstores.file.path': info['scheduler-config']['path']
    }
    sched = Scheduler(config)

    sched.add_cron_job(run_scraper, day_of_week=day_of_week, hour=hour, minute=minute)
    sched.add_cron_job(check_archive, day='first')

    sched.start()

    print('Press Ctrl+{0} to exit'.format('C'))

    try:
        # This is here to simulate application activity (which keeps the main thread alive).
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        sched.shutdown()  # Not strictly necessary if daemonic mode is enabled but should be done if possibleisched.start()

def run_scraper(config_file):
    info = load_config(config_file)
    # does this need to be added to the yaml file? 
    r = requests.post(info['url'] + 'consume')

    
def check_archive():

    all_yamls = {}
    for filename in os.listdir( 'manifests/' ):
        yaml_dict = load_config( 'manifests/' + filename )
        all_yamls[ yaml_dict['directory'] ] = yaml_dict[ 'url' ]

    for dirname, dirnames, filenames in os.walk( '../archive/' ):
        if os.path.isfile( dirname + '/raw.json' ) and not os.path.isfile( dirname + '/parsed.json' ):
            for filename in filenames:
                if 'raw' in filename:
                    with open(os.path.join(dirname, filename), 'r') as f:
                        payload = {'doc': f.read(), 'timestamp': dirname.split('/')[-1]}
                        requests.post(all_yamls[ dirname.split('/')[2] ] + 'process', params=payload)  # TODO

def request_parse(directory):
    all_yamls = {}
    for filename in os.listdir( 'worker_manager/manifests/' ):
        yaml_dict = load_config( 'worker_manager/manifests/' + filename )
        all_yamls[ yaml_dict['directory'] ] = yaml_dict[ 'url' ]


    url = all_yamls[directory.split('/')[1]]

    # logger.warn( 'SCHEDULE!!! ' + url)

    with open(os.path.abspath(directory), 'r') as f:
        logger.warn('path is: ' + os.path.abspath(directory))
        payload = {'doc': f.read(), 'timestamp': directory.split('/')[-2]}
        logger.warn('pAYLOAD!!!!' + str(payload))
        requests.get(url + 'process', params=payload)  # TODO

if __name__ == '__main__':
    # check_archive()
    run_scraper('manifests/plos-manifest.yml')
    # run_scraper('manifests/scitech-manifest.yml')
