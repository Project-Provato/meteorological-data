import scrapy, psycopg2, os, yaml
from datetime import datetime as dt
from dotenv import load_dotenv

load_dotenv() # load environment variables

from ..functions_general.functions import convert_day, convert_hour

class Meteo_Live_Data(scrapy.Spider):
    name = os.path.splitext(os.path.basename(__file__))[0] # specifies the spider name, using the file name without the .py extension

    def __init__(self):
        # loads the configuration file during class initialization

        self.config = self.load_config()

    def parse(self, response): 
        # for every website we scrape, requests are initiated via the 'start_requests' method and each response is processed and returned via the 'parse' method

        if self.config['check_station_availability'] is True:
            if self.init_check_station_availability(response) is True:
                return
            
        start_scraping = self.init_scraping_data(response)
        
        yield from self.yield_all_items(start_scraping)

    def yield_all_items(self, all_measurements):
        yield all_measurements

    def init_check_station_availability(self, response):
        # checks if station is offline or online 
        
        if response.xpath(self.config['meteo_live_data']['offline_station_check']).get() is not None:
            print("Station is offline, skipping...")
            return True
        
        print("Station is online, scraping...")

    def init_scraping_data(self, response):
        # this is the method that initializes the basic data and measurements to be retrieved from meteo
        # it checks from the config if we can retrieve the basic data and the measurement. If it is true, all the basic data and all the measurements for each station are collected using the 'get_data_from_table' method

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
                result = self.get_data_from_table(response, measurement) # returned data: {'measurement': 'value'}

                if result is not None:
                    all_measurements.update(result)

        return all_measurements

    def get_data_from_table(self, response, measurement):
        # this is the method where we retrieve the measurements from meteo
        # we check if the data from meteo contains the words that we have specified in the config, in the 'weather_live_conditions_measurements' field

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
        # method for retrieving the data table from meteo

        return response.xpath(self.config['meteo_live_data']['get_data_table'])
    
    def get_day_and_hour(self, response):
        # method for extracting the day and hour from the meteo table

        return response.xpath(self.config['meteo_live_data']['get_day_and_hour']).get()
    
    def load_config(self):
        # method for loading the configuration file (config.yaml)

        with open(os.getenv('CONFIG'), 'r') as conf:
            return yaml.safe_load(conf)
        
    def start_requests(self):
        # method where scraping begins in scrapy
        # we check in the config in the farms field, which farm has meteo as the source, and we scrape using its URL
        # we provide the url, source, and city through the meta, so that we can use them as values for the basic fields we have defined for scraping. These basic data are defined in the config and set in the 'init_scraping_data' method

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

