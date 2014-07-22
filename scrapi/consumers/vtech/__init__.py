import os
import subprocess

with open(os.path.abspath(os.path.join(os.path.abspath(__file__), os.pardir)) + '/version', 'w') as f:
    f.write(subprocess.check_output(['git', 'rev-parse', 'HEAD']))
