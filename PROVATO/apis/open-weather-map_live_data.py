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
        
        return True if file_is_new is True else False
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

def get_unit(measurement):
    try:
        if measurement is None:
            return None

        UNITS = {
            'temp': '°C',
            'humidity': '%',
            'speed': 'km/h',
            'deg': '°',
            'rain': 'mm',
            'pressure': 'hpa'
        }

        if UNITS.get(measurement) is None:
            return None

        return UNITS.get(measurement)
    except Exception as e:
        print(e)

def find_record(contents, alternative_names, units):
    try:
        for item in alternative_names:
            if units == 'metric':
                if item in ['temp', 'humidity', 'pressure']:
                    return f'{contents['main'][item]}{get_unit(item)}' if contents.get('main', None) is not None and contents.get('main', None).get(item, None) is not None else f'{0.0}{get_unit(item)}'

                if item in ['speed', 'deg']:
                    return f'{contents['wind'][item]}{get_unit(item)}' if contents.get('wind', None) is not None and contents.get('wind', None).get(item, None) is not None else f'{0.0}{get_unit(item)}'

                if item == 'rain':
                    return f'{contents[item]['1h']}{get_unit(item)}' if contents.get(item, None) is not None and contents.get(item, None).get('1h', None) is not None else f'{0.0}{get_unit(item)}'
            else:
                return None
    except Exception as e:
        print(e)

    return None

def get_data_from_open_weather_map():
    try:
        config = load_config()
        
        for farms, farm_data in config.get('farms').items():
            openweathermap_stations = list(filter(lambda find_openweathermap: find_openweathermap.get('code') == config['weather_websites'][4]['code'], farm_data))

            if not openweathermap_stations:
                continue

            for station in openweathermap_stations:
                # print(os.getenv(station.get('url')))

                request = requests.get(os.getenv(station.get('url')), timeout = 10)

                if request.status_code == 200:
                    contents = json.loads(request.content.decode('utf-8'))

                    units = None
                    if 'units=metric' in os.getenv(station.get('url')):
                        units = 'metric'

                    farm = farms
                    source = station['source']
                    timedata = datetime.fromtimestamp(contents['dt']).strftime("%Y-%m-%d %H:%M:%S.%f")
                    crawled = datetime.now()
                    city = station['city']
                    nomos = station['nomos']

                    all_measurements = {}

                    if config['get_weather_basic_data'] is True:
                        all_measurements = {
                            key: locals()[key] for key in config['weather_live_basic_data']
                        }

                    for measurement, alternative_names in config['weather_live_conditions_measurements'].items():
                        all_measurements.update({measurement: find_record(contents, alternative_names, units)})

                    print(all_measurements)

                    staging_path = config['preprocessing']['open-weather-map']['staging']
                    is_new = check_staging_csv(staging_path)

                    with open(staging_path, 'a', encoding = 'utf-8', newline = '') as staging:
                        if is_new is True:
                            csv.writer(staging).writerow(check_header(None, config))

                        csv.writer(staging).writerow(all_measurements.values())
    except Exception as e:
        print(e)

get_data_from_open_weather_map()
