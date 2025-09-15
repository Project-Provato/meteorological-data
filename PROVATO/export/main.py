from dotenv import load_dotenv
load_dotenv() # load environment variables

import yaml, os, requests, logging

from abc import ABC, abstractmethod
from datetime import datetime, timedelta

class WeatherData(ABC):
    def __init__(self):
        self._farm = None
        self._source = None
        self._timedata = None
        self._crawled = datetime.now()
        self._city = None
        self._nomos = None
        
        self._stations = []

        self._all_measurements = {}

        with open(os.getenv('CONFIG'), 'r') as conf:
            self._config = yaml.safe_load(conf)

        # logging.basicConfig(
        #     now = datetime.now()
        #     year = now.strftime("%Y")
        #     month = now.strftime("%m")
        #     day = now.strftime("%d")
        #     filename = '../logs/preprocessing.txt',
        #     level = logging.INFO,
        #     format = '[%(asctime)s] %(levelname)s | Preprocessing | %(message)s',
        #     encoding = 'utf-8'
        # )

    # ------------------------------------

    @property
    def all_measurements(self):
        return self._all_measurements
    
    @property
    def config(self):
        return self._config
    
    @property
    def stations(self):
        return self._stations

    @property
    def farm(self):
        return self._farm

    @property
    def source(self):
        return self._source

    @property
    def timedata(self):
        return self._timedata

    @property
    def crawled(self):
        return self._crawled

    @property
    def city(self):
        return self._city

    @property
    def nomos(self):
        return self._nomos
    
    # ------------------------------------

    def set_farm(self, value):
        self._farm = value

    def set_source(self, value):
        self._source = value

    def set_timedata(self, value):
        self._timedata = value

    def set_city(self, value):
        self._city = value

    def set_nomos(self, value):
        self._nomos = value

    def set_stations(self, stations):
        self._stations.append(stations)

    def set_measurements(self, value):
        self._all_measurements.update(value)

    # ------------------------------------

    def run_basic(self):
        if self.config['get_weather_basic_data'] is True:
            self.set_measurements({
                key: getattr(self, key, None) for key in self.config['weather_live_basic_data']
            })

    def run_measurements_scraping(self, response):
        if self.config['get_weather_measurements'] is True:
            for measurement, measurement_alternative_names in self.config['weather_live_conditions_measurements'].items():
                result = self.get_data(response, measurement, measurement_alternative_names) # returned data: {'measurement': 'value'}

                if result is None:
                    self.set_measurements({measurement: None})
                    continue

                self.set_measurements(result)

    def run_measurements_api(self, name, record):
        if self.config['get_weather_measurements'] is True:
            for measurement, _ in self.config['weather_live_conditions_measurements'].items():
                if measurement == name:
                    self.set_measurements({measurement: record})
                    break

    def run_measurements_api_without_name(self, record):
        if self.config['get_weather_measurements'] is True:
            for measurement, _ in self.config['weather_live_conditions_measurements'].items():
                for item in record:
                    if item == measurement:
                        self.set_measurements({measurement: record[item]})
                        break

    def yield_all_items(self, all_measurements_values):
        yield all_measurements_values

    def init_get_stations(self, source):
        for farm, farm_data in self.config['farms'].items():
            general = list(filter(lambda find_source: find_source.get('code') == self.config['weather_websites'][source]['code'], farm_data))
            
            if general is None or not general:
                continue

            for st in general:
                st['farm'] = farm

            self.set_stations(general)

    def exporter_start_requests(self, source, req):
        for farm, farm_data in self.config['farms'].items():
            data = list(filter(lambda find_source: find_source['code'] == self.config['weather_websites'][source]['code'], farm_data))

            if not data or data is None:
                continue

            for item in data:
                self.set_stations(item)

            for station in self.stations:
                meta_data = {}

                for item in self.config['weather_live_basic_data']:
                    if item == 'farm':
                        meta_data.update({item: farm})
                        continue

                    if station.get(item) is None:
                        meta_data.update({item: None})
                        continue

                    meta_data.update({item: station[item]})

                yield req(station['url'], self.parse, meta = meta_data)

    def exporter_start_requests_api(self):
        if self.stations is None or not self.stations:
            return
        
        for farm in self.stations:
            for station in farm:
                request = None

                if 'SODA' in station.get('url'):
                    start = self.crawled - timedelta(hours = 1, minutes = 30)

                    start_date = start.strftime('%Y-%m-%d %H:%M:%S').replace(' ', '%20')
                    end_date = self.crawled.strftime('%Y-%m-%d %H:%M:%S').replace(' ', '%20')

                    url = station.get('url')

                    request = requests.get(f'{os.getenv(f'{url}_path')}?start_date={start_date}&end_date={end_date}&username={os.getenv(f'{url}_username')}&password={os.getenv(f'{url}_password')}', timeout = 10)
                else:
                    request = requests.get(os.getenv(station.get('url')), timeout = 10)

                yield request, station

                # if request.status_code == 200:
                #     print('elaa')
                # else:
                #     print('skata')

    # def clean_measurements(self):
    #     self.__all_measurements.clear()

    @abstractmethod
    def get_data(self, *args, **kwargs):
        pass

    @abstractmethod
    def parse(self):
        pass
