# from dotenv import load_dotenv
# load_dotenv() # load environment variables

# import os, requests, csv, json
# from datetime import datetime

# from exporter.main import WeatherData

# class OpenWeatherMap(WeatherData):
#     def __init__(self):
#         WeatherData.__init__(self)

#     def check_staging_csv(self, path):
#         try:
#             file_is_new = not os.path.exists(path) or os.path.getsize(path) == 0
            
#             return True if file_is_new is True else False
#         except Exception as e:
#             print(e)
#             return False

#     def check_header(self, header, config):
#         try:    
#             items = []

#             for item in config['weather_live_basic_data']:
#                 items.append(item)

#             for item in config['weather_live_conditions_measurements']:
#                 items.append(item)

#             if not header == items:
#                 return items

#             return header
#         except Exception as e:
#             print(e)
#             return None

#     def get_unit(self, measurement):
#         try:
#             if measurement is None:
#                 return None

#             UNITS = {
#                 'temp': '°C',
#                 'humidity': '%',
#                 'speed': 'km/h',
#                 'deg': '°',
#                 'rain': 'mm',
#                 'pressure': 'hpa'
#             }

#             if UNITS.get(measurement) is None:
#                 return None

#             return UNITS.get(measurement)
#         except Exception as e:
#             print(e)

#     def get_data(self, contents, station):
#         self.set_farm(station.get('farm'))
#         self.set_source(station.get('source'))
#         self.set_timedata(datetime.fromtimestamp(contents['dt']).strftime("%Y-%m-%d %H:%M:%S.%f"))
#         self.set_city(station.get('city'))
#         self.set_nomos(station.get('nomos'))

#         self.run_basic()

#         units = None
#         if 'metric' in os.getenv(station.get('url')):
#             units = 'metric'

#         for name, all_alternative_names in self.get_config()['weather_live_conditions_measurements'].items():
#             for item in all_alternative_names:
#                 data = None

#                 if units == 'metric':
#                     if item in ['temp', 'humidity', 'pressure']:
#                         data = f'{contents['main'][item]}{self.get_unit(item)}' if contents.get('main', None) is not None and contents.get('main', None).get(item, None) is not None else f'{0.0}{self.get_unit(item)}'
#                     elif item in ['speed', 'deg']:
#                         data = f'{contents['wind'][item]}{self.get_unit(item)}' if contents.get('wind', None) is not None and contents.get('wind', None).get(item, None) is not None else f'{0.0}{self.get_unit(item)}'
#                     elif item == 'rain':
#                         data = f'{contents[item]['1h']}{self.get_unit(item)}' if contents.get(item, None) is not None and contents.get(item, None).get('1h', None) is not None else f'{0.0}{self.get_unit(item)}'

#                 if data is None:
#                     continue

#                 self.set_measurements({name: data})

#     def parse(self):
#         self.init_get_stations(4)

#         for request, station in self.exporter_start_requests_api():
#             contents = json.loads(request.content.decode('utf-8'))

#             self.get_data(contents, station)

#             staging_path = self.get_config()['preprocessing']['open-weather-map']['staging']
#             is_new = self.check_staging_csv(staging_path)

#             with open(staging_path, 'a', encoding = 'utf-8', newline = '') as staging:
#                 if is_new is True:
#                     csv.writer(staging).writerow(self.check_header(None))

#                 csv.writer(staging).writerow(self.get_all_measurements().values())

# test = OpenWeatherMap()
# test.parse()