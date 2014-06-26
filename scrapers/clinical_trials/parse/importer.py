from glob import glob
from clinical_trials_parser import json_osf_format
import api.process_docs as process_docs
import sys

reload(sys)
sys.setdefaultencoding('utf-8')
sys.path.insert(1, '/home/faye/cos/scrapi/')

# Takes a python object as a payload

trials = glob('xml/*')
for trial in trials:
    id = trial.split('/')[-1].rstrip('.xml')
    doc = json_osf_format(id)
    if not doc:
        continue
    process_docs.process(doc)