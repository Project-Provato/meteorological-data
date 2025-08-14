from dotenv import load_dotenv
load_dotenv() # load environment variables

import os, yaml, requests, csv, json
from datetime import datetime

def load_config():
    with open(os.getenv('CONFIG'), 'r') as conf:
        return yaml.safe_load(conf)

def check_staging_csv(path):
    try:
        file_is_new = not os.path.exists(path) or os.path.getsize(path) == 0

        return file_is_new
    except Exception as e:
        print(e)

    return False

def check_header(header, config):
    try:    
        items = []

        for item in config['weather_live_basic_data']:
            items.append(item)

        for item in config['weather_live_conditions_measurements']:
            items.append(item)

        if not header == items:
            return items

        return header
    except Exception as e:
        print(e)

    return None

def find_record(contents, alternative_names):
    try:
        current = contents['current']
        units = contents['current_units']

        if current is None:
            return None
        
        find = next((f'{current[item]}{units[item]}' for item in alternative_names if item in current))

        return None if find is None else find
    except Exception as e:
        print(e)

    return None

    # for item in current:
    #     if item in alternative_names:
    #         return current[item]
    
    # return current[measurement]

def get_data_from_open_meteo():
    config = load_config()

    for farms, farm_data in config.get('farms').items():
        openmeteo_stations = list(filter(lambda find_openmeteo: find_openmeteo.get('code') == config['weather_websites'][3]['code'], farm_data))

        if not openmeteo_stations:
            continue

        for station in openmeteo_stations:
            # print(os.getenv(station.get('url')))
            request = requests.get(os.getenv(station.get('url')), timeout = 10)

            if request.status_code == 200:
                all_measurements = {}

                try:
                    contents = json.loads(request.content.decode('utf-8'))

                    farm = farms
                    source = station['source']
                    timedata = datetime.fromtimestamp(contents['current']['time']).strftime("%Y-%m-%d %H:%M:%S.%f")
                    crawled = datetime.now()
                    city = station['city']
                    nomos = station['nomos']

                    if config['get_weather_basic_data'] is True:
                        all_measurements = {
                            key: locals()[key] for key in config['weather_live_basic_data']
                        }

                    for measurement, alternative_names in config['weather_live_conditions_measurements'].items():
                        all_measurements.update({measurement: find_record(contents, alternative_names)})
                except Exception as e:
                    print(e)

                # print(all_measurements)

                try:
                    staging_path = config['preprocessing']['open-meteo']['staging']
                    is_new = check_staging_csv(staging_path)

                    try:
                        with open(staging_path, 'a', encoding = 'utf-8', newline = '') as staging:
                            if is_new is True:
                                csv.writer(staging).writerow(check_header(None, config))

                            csv.writer(staging).writerow(all_measurements.values())
                    except OSError as e:
                        print(e)
                except Exception as e:
                    print(e)
    
get_data_from_open_meteo()
