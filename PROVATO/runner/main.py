from dotenv import load_dotenv
load_dotenv() # load environment variables

import os, yaml, subprocess, time

with open(os.getenv('CONFIG'), 'r') as conf:
    config = yaml.safe_load(conf)

for i in config['commands'].values():
    time.sleep(1)

    if i == 'preprocessing':
        time.sleep(30)
    else:
        time.sleep(5)

    subprocess.run(i, shell = True)
