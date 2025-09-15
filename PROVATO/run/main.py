from dotenv import load_dotenv
load_dotenv() # load environment variables

import os, yaml, subprocess, time

try:
    with open(os.getenv('CONFIG'), 'r') as conf:
        config = yaml.safe_load(conf)

    time.sleep(3)

    for item in config['commands']:
        if item == 'preprocessing':
            time.sleep(15)
        else:
            time.sleep(5)

        subprocess.run(config['commands'][item], shell = True, check = True)

except FileNotFoundError as e:
    print(f"Config file not found: {e}")
except yaml.YAMLError as e:
    print(f"Error parsing YAML file: {e}")
except subprocess.CalledProcessError as e:
    print(f"Command failed: {e.cmd}")
except Exception as e:
    print(f"Unexpected error: {e}")