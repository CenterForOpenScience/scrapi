import sys


class _Registry(dict):

    # These must be defined so that doctest gathering doesn't make
    # pytest crash when trying to figure out what/where scrapi.registry is
    __file__ = __file__
    __name__ = __name__

    def __init__(self):
        dict.__init__(self)

    def __hash__(self):
        return hash(self.freeze(self))

    def __getitem__(self, key):
        try:
            return dict.__getitem__(self, key)
        except KeyError:
            raise KeyError('No harvester named "{}"'.format(key))

    def freeze(self, o):
        if isinstance(o, dict):
            return frozenset({k: self.freeze(v) for k, v in o.items()}.items())
        elif isinstance(o, list):
            return tuple(map(self.freeze, o))
        return o

    @property
    def beat_schedule(self):
        from celery.schedules import crontab
        return {
            'run_{}'.format(name): {
                'args': [name],
                'schedule': crontab(**inst.run_at),
                'task': 'scrapi.tasks.run_harvester',
            }
            for name, inst
            in self.items()
        }

sys.modules[__name__] = _Registry()
