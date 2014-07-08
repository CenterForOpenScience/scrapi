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
    url = info['url'] + 'consume'
    logger.debug('!! Request to run scraper: ' + url)
    requests.post(url)


def check_archive():
    all_yamls = {}
    for filename in os.listdir('worker_manager/manifests/'):
        yaml_dict = load_config('worker_manager/manifests/' + filename)
        all_yamls[yaml_dict['directory']] = yaml_dict['url']

    for dirname, dirnames, filenames in os.walk('archive/'):
        if os.path.isfile(dirname + '/raw' + yaml_dict['file-format']) and not os.path.isfile(dirname + '/parsed.json'):
            for filename in filenames:
                if 'raw' in filename:
                    with open(os.path.join(dirname, filename), 'r') as f:
                        payload = {'doc': f.read(), 'timestamp': dirname.split('/')[-1]}
                        requests.post(all_yamls[dirname.split('/')[2]] + 'process', params=payload)  # TODO


def request_parses(config_file):
    config = load_config(config_file)

    with open('worker_manager/recent_files.txt', 'r') as recent_files:
        for line in recent_files:
            info = line.split(',')
            source = info[0].replace(' ', '')
            doc_id = info[1].replace(' ', '').replace('/', '')
            timestamp = info[2].replace(' ', '', 1).replace('\n', '')
            directory = 'archive/' + source + '/' + doc_id + '/' + timestamp + '/raw' + config['file-format']
            with open(directory, 'r') as f:
                payload = {'doc': f.read(), 'timestamp': timestamp}
                requests.get(config['url'] + 'process', params=payload)

    os.remove('worker_manager/recent_files.txt')

if __name__ == '__main__':
    config = 'worker_manager/manifests/plos-manifest.yml'
    run_scraper(config)
    request_parses(config)
