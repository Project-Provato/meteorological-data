from dotenv import load_dotenv
load_dotenv() # load environment variables

import scrapy, os, yaml
from datetime import datetime as dt

from ..export.main import WeatherData

class Meteo_Live_Data(scrapy.Spider, WeatherData):
    name = os.path.splitext(os.path.basename(__file__))[0] # specifies the spider name, using the file name without the .py extension

    def __init__(self):
        WeatherData.__init__(self)

    def parse(self, response):
        # for every website we scrape, requests are initiated via the 'start_requests' method and each response is processed and returned via the 'parse' method
        
        if self.config['check_station_availability'] is True:
            if self.init_check_station_availability(response) is True:
                return
            
        start_scraping = self.init_scraping_data(response)

        yield from self.yield_all_items(start_scraping)

    def init_check_station_availability(self, response):
        # checks if station is offline or online 
        if response.xpath(self.config['meteo_live_data_paths']['station_availability']).get() is not None:
            print("Station is offline, skipping...")
            return True
        
        print("Station is online, scraping...")

    def init_scraping_data(self, response):
        # this is the method that initializes the basic data and measurements to be retrieved from meteo
        # it checks from the config if we can retrieve the basic data and the measurement. If it is true, all the basic data and all the measurements for each station are collected using the 'get_data_from_meteo' method
        # self.init_get_stations(2)
        
        self.set_farm(response.meta['farm'])
        self.set_source(response.meta['source'])
        self.set_timedata(self.get_day_and_hour(response))
        self.set_city(response.meta['city'])
        self.set_nomos(response.meta['nomos'])

        self.run_basic()

        self.run_measurements_scraping(response)

        return self.all_measurements

    def get_data(self, response, measurement, measurement_alternative_names):
        # this is the method where we retrieve the measurements from meteo
        # we check if the data from meteo contains the words that we have specified in the config, in the 'weather_live_conditions_measurements' field
        
        measurement = measurement.lower()

        for row in self.get_path(response):
            label = row.xpath(self.config['meteo_live_data_paths']['get_data_table_label']).get()
            value = row.xpath(self.config['meteo_live_data_paths']['get_data_table_value']).get()

            if row is None or label is None or value is None:
                continue

            label = label.lower()
            value = value.strip()

            if 'wind' in measurement and label == 'wind' and 'speed' in measurement_alternative_names:
                value = value.split(' ')

                return {measurement: f"{value[0] + value[1]}"}
            
            if 'direction' in measurement and label == 'wind' and 'direction' in measurement_alternative_names:
                return {measurement: value.split(' ')[3]}
            
            if label not in measurement_alternative_names:
                continue

            return {measurement: value}

    def get_path(self, response):
        # method for retrieving the data table from meteo
        return response.xpath(self.config['meteo_live_data_paths']['get_data_table'])
    
    def get_day_and_hour(self, response):
        # method for extracting the day and hour from the meteo table
        return response.xpath(self.config['meteo_live_data_paths']['get_day_and_hour']).get()

    def start_requests(self):
        # method where scraping begins in scrapy
        # we check in the config in the farms field, which farm has meteo as the source, and we scrape using its URL
        # we provide the url, source, and city through the meta, so that we can use them as values for the basic fields we have defined for scraping. These basic data are defined in the config and set in the 'init_scraping_data' method
        yield from self.exporter_start_requests(2, scrapy.Request)
