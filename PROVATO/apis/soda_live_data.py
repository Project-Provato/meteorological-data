from dotenv import load_dotenv
load_dotenv() # load environment variables

import os, yaml, requests, csv
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

def get_weather_sensor_path(xml_data):
    root = ET.fromstring(xml_data)

    for weather_sensor_path in root.findall('.//unita'):
        if int(weather_sensor_path.get('id')) in config['soda_api_paths']['weather_sensor_id_path']:
            return weather_sensor_path
        
    return None

def find_record(measurement_label):
    barometer_sensor = None

    for sensor in measurement_label.findall('sensore'):
        if int(sensor.get('id')) in config['soda_api_paths']['barometer']:
            barometer_sensor = sensor
            break

    if barometer_sensor is None:
        return None

    measurement = barometer_sensor.findall('misura')

    if measurement is None:
        return None

    measurement = measurement[-1]
    measurement_date_string = measurement.get('data_ora')
    measurement_date = datetime.strptime(measurement_date_string, '%Y-%m-%d %H:%M:%S')
    tolerance = timedelta(minutes = 1, seconds = 30)
    result = {}
    time = None

    for key, possible_names in config['soda_api_paths'].items():
        if 17 in possible_names: # (id)
            continue

        found = False

        for sensors in measurement_label.findall('sensore'):
            id_sensor = int(sensors.get('id'))

            if id_sensor in possible_names:
                value = None
                min_time_difference = None

                for measurement in sensors.findall('misura'):
                    date_hour_path = measurement.get('data_ora')
                
                    try:
                        dt = datetime.strptime(date_hour_path, '%Y-%m-%d %H:%M:%S')
                        time_difference = abs(dt - measurement_date)
                
                        if time_difference <= tolerance and (min_time_difference is None or time_difference < min_time_difference):
                            value = measurement.get('valore')
                            time = date_hour_path
                            min_time_difference = time_difference
                    except Exception as e:
                        print(e)

                if value is None:
                    return None

                result[key] = value
                found = True
                break

        if found is None or time is None:
            return None

    # print(result, time)

    return result, time

def get_data_from_soda():
    now = datetime.now().replace(second = 0, microsecond = 0)
    start = now - timedelta(hours = 1, minutes = 45)
    
    start_date = start.strftime('%Y-%m-%d %H:%M:%S').replace(' ', '%20')
    end_date = now.strftime('%Y-%m-%d %H:%M:%S').replace(' ', '%20')

    for farms, farm_data in config.get('farms').items():
        soda_stations = list(filter(lambda find_soda: find_soda.get('code') == config['weather_websites'][0]['code'], farm_data))

        # print(soda_stations)

        if soda_stations is None:
            return None

        for station in soda_stations:
            url = station.get('url')
            path = os.getenv(f'{url}_path')
            username = os.getenv(f'{url}_username')
            password = os.getenv(f'{url}_password')

            url = f'{path}?start_date={start_date}&end_date={end_date}&username={username}&password={password}'
            
            # print(url)

            try:
                request = requests.get(url, timeout = 10)

                if request.status_code == 200:
                    weather_sensors = get_weather_sensor_path(request.content.decode('utf-8'))

                    if weather_sensors is not None:
                        record, timestamp = find_record(weather_sensors)

                        if record is None or timestamp is None:
                            return None
                    
                        farm = farms
                        source = station['source']
                        timedata = timestamp
                        crawled = datetime.now()
                        city = station['city']
                        nomos = station['nomos']

                        all_measurements = {}

                        if config['get_weather_basic_data'] is True:
                            all_measurements = {
                                key: locals()[key] for key in config['weather_live_basic_data']
                            }

                        for key, value in record.items():
                            all_measurements.update({key: value})

                        values = list(all_measurements.values())

                        with open(config['preprocessing']['soda']['staging'], 'a', encoding = 'utf-8', newline = '') as staging:
                            csv.writer(staging).writerow(values)
            except Exception as e:
                print(f'Request failed: {e}')

try:
    with open(os.getenv('CONFIG'), 'r') as conf:
        config = yaml.safe_load(conf)
    
    get_data_from_soda()
except Exception as e:
    print(e)

