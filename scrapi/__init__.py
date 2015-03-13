from celery.schedules import crontab

from scrapi import linter, requests, database  # noqa


class _Registry(dict):

    def __init__(self):
        super(_Registry, self).__init__()

    def collect_harvesters(self):
        pass

    def __getitem__(self, key):
        try:
            return super(_Registry, self).__getitem__(key)
        except KeyError:
            raise KeyError('No harvester named "{}"'.format(key))

    @property
    def beat_schedule(self):
        return {
            'run_{}'.format(name): {
                'args': [name],
                'schedule': crontab(**inst.run_at),
                'task': 'scrapi.tasks.run_harvester',
            }
            for name, inst
            in self.items()
        }

registry = _Registry()
