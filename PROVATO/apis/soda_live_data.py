from dotenv import load_dotenv
load_dotenv() # load environment variables

import os, yaml, requests, csv
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

def load_config():
    try:
        with open(os.getenv('CONFIG'), 'r') as conf:
            return yaml.safe_load(conf)
    except FileNotFoundError as e:
        print(e)

def get_weather_sensor_path(xml_data, config):
    try:
        root = ET.fromstring(xml_data)

        for weather_sensor_path in root.findall('.//unita'):
            if int(weather_sensor_path.get('id')) in config['soda_api_paths']['weather_sensor_id_path']:
                return weather_sensor_path
    except Exception as e:
        print(e)

        return None

def find_record(measurement_label, config):
    barometer_sensor = None

    try:
        for sensor in measurement_label.findall('sensore'):
            if int(sensor.get('id')) in config['soda_api_paths']['barometer']:
                barometer_sensor = sensor
                break
    except Exception as e:
        print(e)

        return None

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
            unit = sensors.get('unita')

            if id_sensor in possible_names:
                value = None
                min_time_difference = None

                for measurement in sensors.findall('misura'):
                    date_hour_path = measurement.get('data_ora')
                
                    try:
                        dt = datetime.strptime(date_hour_path, '%Y-%m-%d %H:%M:%S')
                        time_difference = abs(dt - measurement_date)
                
                        if time_difference <= tolerance and (min_time_difference is None or time_difference < min_time_difference):
                            value = f"{measurement.get('valore')}{unit}"
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

def get_paths(station):
    url = station.get('url')

    path = os.getenv(f'{url}_path')
    username = os.getenv(f'{url}_username')
    password = os.getenv(f'{url}_password')

    return path, username, password

def get_basic_data(farms, station, timestamp, now):
    try:
        farm = farms
        source = station['source']
        timedata = timestamp
        crawled = now
        city = station['city']
        nomos = station['nomos']

        return farm, source, timedata, crawled, city, nomos
    except Exception as e:
        print(e)

    return None

def get_data_from_soda():
    config = load_config()

    now = datetime.now()
    start = now - timedelta(hours = 1, minutes = 30)
    
    start_date = start.strftime('%Y-%m-%d %H:%M:%S').replace(' ', '%20')
    end_date = now.strftime('%Y-%m-%d %H:%M:%S').replace(' ', '%20')

    for farms, farm_data in config.get('farms').items():
        soda_stations = list(filter(lambda find_soda: find_soda.get('code') == config['weather_websites'][0]['code'], farm_data))

        if not soda_stations:
            continue

        for station in soda_stations:
            try:
                path, username, password = get_paths(station)

                full_url = f'{path}?start_date={start_date}&end_date={end_date}&username={username}&password={password}'

                # print(url)

                request = requests.get(full_url, timeout = 10)

                if request.status_code == 200:
                    weather_sensors = get_weather_sensor_path(request.content.decode('utf-8'), config)

                    if weather_sensors is not None:
                        record, timestamp = find_record(weather_sensors, config)

                        if record is None or timestamp is None:
                            return None
                        
                        farm, source, timedata, crawled, city, nomos = get_basic_data(farms, station, timestamp, now)

                        all_measurements = {}

                        if config['get_weather_basic_data'] is True:
                            all_measurements = {
                                key: locals()[key] for key in config['weather_live_basic_data']
                            }

                        for measurement, _ in config['weather_live_conditions_measurements'].items():
                            all_measurements.update({measurement: record[measurement]})

                        try:
                            with open(config['preprocessing']['soda']['staging'], 'a', encoding = 'utf-8', newline = '') as staging:
                                csv.writer(staging).writerow(all_measurements.values())
                        except FileNotFoundError as e:
                            print(e)
            except Exception as e:
                print(e)

get_data_from_soda()
