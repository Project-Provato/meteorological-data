from dotenv import load_dotenv
load_dotenv() # load environment variables

import scrapy, os, re
from datetime import datetime as dt

from ..export.main import WeatherData

class WU_Live_Data(scrapy.Spider, WeatherData):
    name = os.path.splitext(os.path.basename(__file__))[0] # specifies the spider name, using the file name without the .py extension

    def __init__(self):
        WeatherData.__init__(self)

    def parse(self, response): 
        # for every website we scrape, requests are initiated via the 'start_requests' method and each response is processed and returned via the 'parse' method
        # if self.config['check_station_availability'] is True:
        #     if self.init_check_station_availability(response) is True:
        #         return
            
        start_scraping = self.init_scraping_data(response)
        
        yield from self.yield_all_items(start_scraping)

    # def init_check_station_availability(self, response):
    #     # checks if station is offline or online 
        
    #     if response.xpath(self.config['weather-underground_live_data_paths']['station_availability']).get() is not None:
    #         print("Station is offline, skipping...")
    #         return True
        
    #     print("Station is online, scraping...")

    def init_scraping_data(self, response):
        # this is the method that initializes the basic data and measurements to be retrieved from weather-underground
        # it checks from the config if we can retrieve the basic data and the measurement. If it is true, all the basic data and all the measurements for each station are collected using the 'get_data_from_wu' method
        self.init_get_stations(2)
        
        self.set_farm(response.meta['farm'])
        self.set_source(response.meta['source'])
        self.set_timedata(self.get_day_and_hour(response))
        self.set_city(response.meta['city'])
        self.set_nomos(response.meta['nomos'])

        self.run_basic()

        self.run_measurements_scraping(response)

        return self.all_measurements
    
    def get_day_and_hour(self, response):
        return response.xpath(self.config['weather-underground_live_data_paths']['get_day_and_hour']).get()
    
    def get_data(self, response, measurement, measurement_alternative_names):
        measurement = measurement.lower()
        measurement_alternative_names = [word.lower() for word in measurement_alternative_names]

        for item in self.config['weather-underground_live_data_paths']:
            if not item in measurement_alternative_names:
                continue

            if item == 'direction':
                return self.get_wind_direction(response, measurement)

            return self.get_value_and_unit(response, measurement)

    def get_wind_direction(self, response, measurement):
        # https://en.wikipedia.org/wiki/Cardinal_direction
        # https://en.wikipedia.org/wiki/Compass_rose
        # https://stackoverflow.com/questions/7490660/converting-wind-direction-in-angles-to-text-words
        path = response.xpath(self.config['weather-underground_live_data_paths'][measurement]['value']).get()
        
        match_with_path = re.search(r'rotate\(([\d.]+)deg\)', path)

        if match_with_path is None:
            return None

        return {measurement: float(match_with_path.group(1))} # degrees
    
    def get_value_and_unit(self, response, measurement):
        value = response.xpath(self.config['weather-underground_live_data_paths'][measurement]['value']).get()

        if value is None:
            return None
        
        if self.config['weather-underground_live_data_paths'][measurement]['unit'] is None:
            return value
        
        unit = response.xpath(self.config['weather-underground_live_data_paths'][measurement]['unit']).get()

        if unit is None and measurement == 'wind':
            unit = 'mph'
            
        print({measurement: f'{value}{unit}'})

        return {measurement: f'{value}{unit}'}
        
    def start_requests(self):
        # method where scraping begins in scrapy
        # we check in the config in the farms field, which farm has meteo as the source, and we scrape using its URL
        # we provide the url, source, and city through the meta, so that we can use them as values for the basic fields we have defined for scraping. These basic data are defined in the config and set in the 'init_scraping_data' method
        yield from self.exporter_start_requests(1, scrapy.Request)