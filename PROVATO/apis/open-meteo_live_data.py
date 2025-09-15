from dotenv import load_dotenv
load_dotenv() # load environment variables

import os, csv, json
from datetime import datetime

from export.main import WeatherData

class OpenMeteo(WeatherData):
    def __init__(self):
        WeatherData.__init__(self)

    def check_staging_csv(self, path):
        try:
            file_is_new = not os.path.exists(path) or os.path.getsize(path) == 0

            return file_is_new
        except Exception as e:
            print(e)

        return False

    def check_header(self, header):
        try:    
            items = []

            for item in self.config['weather_live_basic_data']:
                items.append(item)

            for item in self.config['weather_live_conditions_measurements']:
                items.append(item)

            if not header == items:
                return items

            return header
        except Exception as e:
            print(e)

        return None
    
    def run(self, current, units):
        for item in current:
            for name, all_alternative_names in self.config['weather_live_conditions_measurements'].items():
                for alternative_name in all_alternative_names:
                    if alternative_name == item:
                        yield name, f'{current[item]}{units[item]}'

    def get_data(self, request):
        contents = json.loads(request.content.decode('utf-8'))

        current = contents.get('current')
        units = contents.get('current_units')

        if current is None:
            return None

        find = {}

        for name, data in self.run(current, units):
            find.update({name: data})

        if not find:
            return None

        return contents, find

    def parse(self):
        self.init_get_stations(3)

        for request, station in self.exporter_start_requests_api():
            contents, data = self.get_data(request)

            self.set_farm(station.get('farm'))
            self.set_source(station.get('source'))
            self.set_timedata(datetime.fromtimestamp(contents['current']['time']).strftime("%Y-%m-%d %H:%M:%S.%f"))
            self.set_city(station.get('city'))
            self.set_nomos(station.get('nomos'))

            self.run_basic()
            self.run_measurements_api_without_name(data)

            self.set_measurements({'dew_point': None})
            self.set_measurements({'heat_index': None})
            self.set_measurements({'wind_chill': None})
            self.set_measurements({'solar_radiation': None})

            staging_path = self.config['preprocessing']['open-meteo']['staging']
            is_new = self.check_staging_csv(staging_path)

            try:
                with open(staging_path, 'a', encoding = 'utf-8', newline = '') as staging:
                    if is_new is True:
                        csv.writer(staging).writerow(self.check_header(None))

                    csv.writer(staging).writerow(self.all_measurements.values())
            except OSError as e:
                print(e)

test = OpenMeteo()
test.parse()