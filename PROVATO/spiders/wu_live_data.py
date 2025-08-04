from dotenv import load_dotenv
load_dotenv() # load environment variables

import scrapy, os, yaml, re
from datetime import datetime as dt

class Meteo_Live_Data(scrapy.Spider):
    name = os.path.splitext(os.path.basename(__file__))[0] # specifies the spider name, using the file name without the .py extension

    def __init__(self):
        # loads the configuration file during class initialization
        self.config = self.load_config()

    def parse(self, response): 
        # for every website we scrape, requests are initiated via the 'start_requests' method and each response is processed and returned via the 'parse' method
        # if self.config['check_station_availability'] is True:
        #     if self.init_check_station_availability(response) is True:
        #         return
            
        start_scraping = self.init_scraping_data(response)
        
        yield from self.yield_all_items(start_scraping)

    def yield_all_items(self, all_measurements):
        yield all_measurements

    # def init_check_station_availability(self, response):
    #     # checks if station is offline or online 
        
    #     if response.xpath(self.config['weather-underground_live_data_paths']['station_availability']).get() is not None:
    #         print("Station is offline, skipping...")
    #         return True
        
    #     print("Station is online, scraping...")

    def init_scraping_data(self, response):
        # this is the method that initializes the basic data and measurements to be retrieved from weather-underground
        # it checks from the config if we can retrieve the basic data and the measurement. If it is true, all the basic data and all the measurements for each station are collected using the 'get_data_from_wu' method
        source = response.meta['source']
        farm = response.meta['farm']
        timedata = self.get_day_and_hour(response)
        crawled = dt.now()
        city = response.meta['city']

        all_measurements = {}

        if self.config['get_weather_basic_data'] is True:
            all_measurements = {
                key: locals()[key] for key in self.config['weather_live_basic_data']
            }

        for measurement, alternative_names in self.config['weather_live_conditions_measurements'].items():
            measurement_data = self.get_data_from_wu(response, measurement, alternative_names)

            if measurement_data is None:
                continue

            all_measurements.update(
                {measurement: measurement_data}
            )

        return all_measurements
    
    def get_day_and_hour(self, response):
        return response.xpath(self.config['weather-underground_live_data_paths']['get_day_and_hour']).get()
    
    def get_data_from_wu(self, response, measurement, measurement_alternative_names):
        measurement = measurement.lower()
        measurement_alternative_names = [word.lower() for word in measurement_alternative_names]

        for item in self.config['weather-underground_live_data_paths']:
            if not item in measurement_alternative_names:
                continue

            if item == 'wind_direction':
                return self.get_wind_direction(response, measurement)

            return self.get_value_and_unit(response, measurement)

    def get_wind_direction(self, response, measurement):
        # https://en.wikipedia.org/wiki/Cardinal_direction
        # https://en.wikipedia.org/wiki/Compass_rose
        # https://stackoverflow.com/questions/7490660/converting-wind-direction-in-angles-to-text-words
        path = response.xpath(self.config['weather-underground_live_data_paths'][measurement]['value']).get()
        match_with_path = re.search(r'rotate\(([\d.]+)deg\)', path)

        if not match_with_path:
            return None

        degrees = float(match_with_path.group(1))

        compass_sectors = [
            "N", "NNE", "NE", "ENE",
            "E", "ESE", "SE", "SSE",
            "S", "SSW", "SW", "WSW",
            "W", "WNW", "NW", "NNW"
        ]
        
        sector = int((degrees + 11.25) // 22.5) % 16
        
        return compass_sectors[sector]
    
    def get_value_and_unit(self, response, measurement):
        value = response.xpath(self.config['weather-underground_live_data_paths'][measurement]['value']).get()

        if value is None:
            return None
        
        if self.config['weather-underground_live_data_paths'][measurement]['unit'] is None:
            return value
        
        unit = response.xpath(self.config['weather-underground_live_data_paths'][measurement]['unit']).get()

        return f"{value + unit}"
    
    def load_config(self):
        # method for loading the configuration file (config.yaml)
        with open(os.getenv('CONFIG'), 'r') as conf:
            return yaml.safe_load(conf)
        
    def start_requests(self):
        # method where scraping begins in scrapy
        # we check in the config in the farms field, which farm has weather-underground as the source, and we scrape using its URL
        # we provide the url, source, and city through the meta, so that we can use them as values for the basic fields we have defined for scraping. These basic data are defined in the config and set in the 'init_scraping_data' method
        for farm, farm_data in self.config.get('farms').items():
            websites = self.config['weather_websites']

            wu_stations = list(filter(lambda find_wu: find_wu.get('code') == websites[1]['code'], farm_data))

            if wu_stations is None:
                return
            
            for station in wu_stations:                
                meta_data = {}
                
                for item in self.config['weather_live_basic_data']:
                    meta_data.update( {item: farm} ) if item == 'farm' else meta_data.update( {item: station.get(item)} )
                
                yield scrapy.Request(station.get('url'), self.parse, meta = meta_data)

