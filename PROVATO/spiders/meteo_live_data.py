from dotenv import load_dotenv
load_dotenv() # load environment variables

import scrapy, psycopg2, os, yaml
from datetime import datetime as dt

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
        
        if response.xpath(self.config['meteo_live_data_paths']['station_availability']).get() is not None:
            print("Station is offline, skipping...")
            return True
        
        print("Station is online, scraping...")

    def init_scraping_data(self, response):
        # this is the method that initializes the basic data and measurements to be retrieved from meteo
        # it checks from the config if we can retrieve the basic data and the measurement. If it is true, all the basic data and all the measurements for each station are collected using the 'get_data_from_table' method

        source = response.meta['source']
        city = response.meta['city']
        timecrawl = dt.now()
        farm_number = response.meta['farm_number']
        station_number = response.meta['station_number']
        last_station_update = self.get_day_and_hour(response)

        if self.config['get_weather_basic_data'] is True:
            all_measurements = {
                key: locals()[key]
                for key in self.config['weather_live_basic_data']
            }

        if self.config['get_weather_measurements'] is True:
            for measurement, alternative_names in self.config['weather_live_conditions_measurements'].items():
                print(measurement, alternative_names)
                result = self.get_data_from_table(response, measurement, alternative_names) # returned data: {'measurement': 'value'}

                if result is not None:
                    all_measurements.update(result)

        return all_measurements

    def get_data_from_table(self, response, measurement, measurement_alternative_names):
        # this is the method where we retrieve the measurements from meteo
        # we check if the data from meteo contains the words that we have specified in the config, in the 'weather_live_conditions_measurements' field

        measurement = measurement.lower()
        measurement_alternative_names = [word.lower() for word in measurement_alternative_names]

        for row in self.get_data_table(response):
            label = row.xpath(self.config['meteo_live_data_paths']['get_data_table_label']).get()
            value = row.xpath(self.config['meteo_live_data_paths']['get_data_table_value']).get()

            if row is None or label is None or value is None:
                continue

            label = label.lower()
            value = value.strip()

            if 'wind' in measurement and label == 'wind' and 'speed' in measurement_alternative_names:
                value = value.split(' ')

                return {measurement: f"{value[0] + value[1]}"}
            
            if 'wind' in measurement and label == 'wind' and 'direction' in measurement_alternative_names:
                return {measurement: value.split(' ')[3]}
            
            if label not in measurement_alternative_names:
                continue

            return {measurement: value}

    def get_data_table(self, response):
        # method for retrieving the data table from meteo

        return response.xpath(self.config['meteo_live_data_paths']['get_data_table'])
    
    def get_day_and_hour(self, response):
        # method for extracting the day and hour from the meteo table

        return response.xpath(self.config['meteo_live_data_paths']['get_day_and_hour']).get()
    
    def load_config(self):
        # method for loading the configuration file (config.yaml)
        with open(os.getenv('CONFIG'), 'r') as conf:
            return yaml.safe_load(conf)
        
    def start_requests(self):
        # method where scraping begins in scrapy
        # we check in the config in the farms field, which farm has meteo as the source, and we scrape using its URL
        # we provide the url, source, and city through the meta, so that we can use them as values for the basic fields we have defined for scraping. These basic data are defined in the config and set in the 'init_scraping_data' method

        for farm, farm_data in self.config.get('farms').items():
            meteo_stations = list(filter(lambda find_meteo: find_meteo.get('code') == self.config['weather_websites'][2]['code'], farm_data))

            if meteo_stations is None:
                return
            
            for station in meteo_stations:
                meta_data = {}
                
                for item in self.config['weather_live_basic_data']:
                    meta_data.update( {item: farm} ) if item == 'farm_number' else meta_data.update( {item: station.get(item)} )
                
                yield scrapy.Request(station.get('url'),
                                    self.parse,
                                    meta = meta_data)

