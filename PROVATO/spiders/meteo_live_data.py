import scrapy, psycopg2, os, yaml
from datetime import datetime as dt
from dotenv import load_dotenv

load_dotenv()

from ..functions_general.functions import convert_day, convert_hour

class Meteo_Live_Data(scrapy.Spider):
    name = os.path.splitext(os.path.basename(__file__))[0]

    def __init__(self):
        self.config = self.load_config()

    def parse(self, response):
        if self.config['check_station_availability'] is True:
            if self.init_check_station_availability(response) is True:
                return
            
        start_scraping = self.init_scraping_data(response)
        
        yield from self.yield_all_items(start_scraping)

    def yield_all_items(self, all_measurements):
        yield all_measurements

    def init_check_station_availability(self, response):
        if response.xpath(self.config['meteo_live_data']['offline_station_check']).get() is not None:
            print("Station is offline, skipping...")
            return True
        
        print("Station is online, scraping...")

    def init_scraping_data(self, response):
        source = response.meta['source']
        city = response.meta['city']
        timecrawl = dt.now()
        day = convert_day(self.get_day_and_hour(response))
        hour = convert_hour(self.get_day_and_hour(response))

        if self.config['get_weather_basic_data'] is True:
            all_measurements = {
                key: locals()[key]
                for key in self.config['weather_live_basic_data']
            }

        if self.config['get_weather_measurements'] is True:
            for measurement in self.config['weather_live_conditions_measurements']:
                result = self.get_data_from_table(response, measurement)

                if result is not None:
                    all_measurements.update(result)

        return all_measurements

    def get_data_from_table(self, response, measurement):
        for row in self.get_data_table(response):
            label = row.xpath(self.config['meteo_live_data']['get_data_table_label']).get()
            value = row.xpath(self.config['meteo_live_data']['get_data_table_value']).get()

            if row is None or label is None or label is None:
                continue

            label = label.lower()
            measurement = measurement.lower()
            value = value.strip()

            if 'wind' in measurement and label == 'wind' and 'speed' in measurement:
                return {measurement: value.split(' ')[0]}
            
            if 'wind' in measurement and label == 'wind' and 'direction' in measurement:
                return {measurement: value.split(' ')[3]}
            
            if label != measurement:
                continue

            return {measurement: value}

    def get_data_table(self, response):
        return response.xpath(self.config['meteo_live_data']['get_data_table'])
    
    def get_day_and_hour(self, response):
        return response.xpath(self.config['meteo_live_data']['get_day_and_hour']).get()
    
    def load_config(self):
        with open(os.getenv('CONFIG'), 'r') as conf:
            return yaml.safe_load(conf)

    def start_requests(self):
        for farms, farm_data in self.config.get('farms').items():
            meteo_stations = list(filter(lambda find_meteo: find_meteo.get('source') == 'meteo', farm_data))

            if meteo_stations is None:
                return
            
            for station in meteo_stations:
                yield scrapy.Request(station.get('url'),
                                    self.parse,
                                    meta = {'url': station.get('url'),
                                            'source': station.get('source'),
                                            'city': station.get('city')
                                            })

