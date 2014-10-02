import os
import json

from dateutil.parser import parse

from scrapi.util import safe_filename


def migrate_from_old_scrapi():
    for dirname, dirs, filenames in os.walk('archive'):
        for filename in filenames:
            oldpath = os.path.join(dirname, filename)
            source, sid, dt = dirname.split('/')[1:]
            dt = parse(dt).isoformat()
            sid = safe_filename(sid)
            newpath = os.path.join('archive', source, sid, dt, filename)

            if filename == 'manifest.json':
                with open(oldpath) as old:
                    old_json = json.load(old)
                new_json = {
                    'consumerVersion': old_json['version'],
                    'normalizeVersion': old_json['version'],
                    'timestamp': dt,
                    'source': source,
                    'id': sid
                }
                old_json = json.dumps(old_json, indent=4, sort_keys=True)
                new_json = json.dumps(new_json, indent=4, sort_keys=True)

            print '{} -> {}'.format(oldpath, newpath)
            print old_json
            print new_json
