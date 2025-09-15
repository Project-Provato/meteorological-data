from dotenv import load_dotenv
load_dotenv() # load environment variables

import csv
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

from export.main import WeatherData

class Soda_Live_Data(WeatherData):
    def __init__(self):
        WeatherData.__init__(self)

    def get_data(self, xml_data):
        try:
            root = ET.fromstring(xml_data)

            for weather_sensor_path in root.findall('.//unita'):
                if int(weather_sensor_path.get('id')) in self.config['soda_api_paths']['weather_sensor_id_path']:
                    return weather_sensor_path
        except Exception as e:
            print(e)

            return None

    def find_record(self, measurement_label):
        barometer_sensor = None

        try:
            for sensor in measurement_label.findall('sensore'):
                barometer = self.config['weather_live_conditions_measurements']['barometer']

                for possible_names in barometer:
                    if isinstance(possible_names, int) and (int(sensor.get('id')) == possible_names):
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
        time = None
    
        for key, possible_names in self.config['weather_live_conditions_measurements'].items():
            found = False

            for sensors in measurement_label.findall('sensore'):
                id_sensor = int(sensors.get('id'))
                unit = sensors.get('unita')

                for possible_name in possible_names:
                    if not (isinstance(possible_name, int) and id_sensor == possible_name):
                        continue

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
                                found = True
                        except Exception as e:
                            print(e)

                    self.set_measurements({key: value})
                    break

            if found is False:
                self.set_measurements({key: None})

            self.all_measurements['timedata'] = time

        return None

    def parse(self):
        self.init_get_stations(0)

        for request, station in self.exporter_start_requests_api():
            self.set_farm(station.get('farm'))
            self.set_source(station['source'])
            self.set_city(station['city'])
            self.set_nomos(station['nomos'])

            self.run_basic()

            weather_sensors = self.get_data(request.content.decode('utf-8'))

            if weather_sensors is None:
                return None
            
            self.find_record(weather_sensors)

            try:
                with open(self.config['preprocessing']['soda']['staging'], 'a', encoding = 'utf-8', newline = '') as staging:
                    csv.writer(staging).writerow(self.all_measurements.values())
            except FileNotFoundError as e:
                print(e)

test = Soda_Live_Data()
test.parse()