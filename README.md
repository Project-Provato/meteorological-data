# PROVATO - Meteorological Data Collection & Cleaning Pipeline

In this repository, you will find all the tools and methods we use to automate the collection, storage, and cleaning of weather data from different sources—including [Meteo](https://meteo.gr/), [WeatherUnderground](https://www.wunderground.com/) (via scraping), and the [SoDa](https://soda.dit.uop.gr/) API.

We automatically collect weather data from various online platforms. These sources tell us where each weather station is and what measurements it records. By gathering all this information, we can really understand the local conditions for every farm we study.

Our workflow makes sure all weather data gets checked, cleaned up, and saved in a format that’s ready for analysis. At the same time, we always keep the original raw records, so nothing is lost and everything can be double-checked if needed.

## Tools & Frameworks Used
We use Scrapy in Python for web scraping. [Scrapy](https://www.scrapy.org/) is a powerful framework that helps us quickly collect and organize data from websites. It can extract information from web pages and save it in easy-to-use formats like CSV or JSON. In our project, we will use CSV files to store the data we collect.

### Scrapy Project Structure
We created our Scrapy project (`PROVATO`) using the following command:

```bash
scrapy startproject PROVATO
```

When we ran the above command to create our Scrapy project (`PROVATO`), it came with these main files and folders:

- `scrapy.cfg`  
  The main configuration file for the whole project.

- `/PROVATO/`  
  The main project folder, which contains:

  - `spiders/`  
    This folder has all our spiders, which do the actual web scraping. Each spider collects data from a different website.

  - `items.py`  
    Defines the structure (fields) of the data you want to collect (we don’t use this, our data structure is handled in our custom code/config instead).

  - `pipelines.py`  
    Contains code for processing, cleaning, and saving our scraped data (not used, we process and save our data with our own custom code).

  - `settings.py`  
    All the main settings for our project (pipelines, delays, user agents, etc).

  - `middlewares.py`  
    Custom code to control requests and responses as they pass through Scrapy.

## Project Overview
We wanted a system that’s easy to manage and change, so we built everything around a single configuration file. This lets us quickly update which websites we pull data from, what information we collect, and how each step works (without ever touching the code).

Here’s how the whole process works, step by step:

1. **Data Scraping:** First, we automatically collect raw weather data from the chosen websites. At this point, we just grab whatever is there (no checks or changes yet).

2. **Raw Data Staging:** All the raw data goes straight into a temporary CSV file. This file keeps the data exactly as we found it, before any cleaning.

3. **Data Cleaning & Preprocessing:** Next, a Python script takes every record from the staging CSV file:
   - Converting units (for example, fahrenheit to celsius)
   - Making dates and times consistent (like turning them all into the same format)
   - Removing empty, duplicate, or messy rows
   - Making sure every required field is filled out and in the right format  
   
   The cleaned data is then saved to a new CSV file (cleaned CSV file).

4. **Staging Cleanup:** Once cleaning is done, we remove those records from the staging file, so we don’t accidentally process them twice.

5. **Raw Data Archive:** Every raw record is also added to a big archive file. This way, we always have a complete backup of everything we’ve ever collected.

6. **Final Data Import:** Finally, the cleaned data goes into our SQL database.

### The configuration file specifies:

This section lists all the websites we collect weather data from. For each source, we set its name, URL, unique code, and the spider/script name used to grab its data:

```yaml
weather_websites:
  - source: SoDa
    url: https://soda.dit.uop.gr/
    code: soda
    spider_name: soda_live_data

  - source: WeatherUnderground
    url: https://www.wunderground.com/
    code: wu
    spider_name: wu_live_data

  - source: Meteo
    url: https://meteo.gr/
    code: meteo
    spider_name: meteo_live_data
```

### Which Data to Collect

We choose which basic fields and measurements to collect by editing the config file. Comment out any field won’t appear in the results:

- `weather_live_basic_data`: Which fields to collect
- `weather_live_conditions_measurements`: Which measurements to parse

### Which Scripts to Run
Each data source can have its own spider. You control this assignment directly in the config.

### Checkers

These config options let you turn specific features on or off, simply by setting them to True or False:

- `check_station_availability`: If True, the script checks if each weather station is online before scraping
- `get_weather_basic_data`: If True, basic weather data fields will be collected (as set in weather_live_basic_data)
- `get_weather_measurements`: If True, additional weather measurements will be collected (as set in weather_live_conditions_measurements)

> This setup makes it easy to adjust what data is gathered and how the workflow operates, just by editing the configuration file.

### Farms
Each farm has its own section in the config, containing:

- The URLs of the weather stations we collect data from
- The city/location of the farm
- The station code (matching the code defined in weather_websites in the config)
- The data source (i.e., which website the data comes from)
- The unique ID/number of each weather station in every farm

> This setup helps us organize and control exactly where and how we collect weather data for each farm.

### Website Data Extraction Paths
To collect the right data, we need to know where each value is in the website’s HTML. All the required HTML/XPath paths for each website are defined in the config (`meteo_live_data_paths`, `underground_live_data_paths`). They are required for the script to access and collect all the weather measurements and basic data we have defined in weather_live_conditions_measurements and weather_live_basic_data:

- `meteo_live_data_paths`: includes all the XPaths used to extract information from the Meteo website
- `weather-underground_live_data_paths`: contains the XPaths for data from Weather Underground website

> By setting these paths in the config, we make sure our scripts can reliably find and collect the right data from each site. If a website changes its structure, you only need to update the paths here.

### Units

Set our preferred output units for every field in the `results_units` section. The program will automatically convert all values to these units:

```yaml
results_units:
  temperature: celsius
  humidity: percentage
  wind_speed: km/h
  wind_direction: null
  rainfall: mm
  weather_conditions: null
  heat_index: celsius
  pressure: hpa
```

## Results

After running the spiders, you get a list of records for each weather station. The fields in each result depend on what you’ve selected in the config file — only the fields that are active (not commented out) will be included.

**Common fields:**
- `source` — The website or data provider (e.g. Meteo, WeatherUnderground)
- `city` — The city or location of the weather station
- `timecrawl` — The exact time the data was collected (timestamp)
- `last_station_update` — The most recent update time reported by the station
- `farm_number` — The farm where the station belongs
- `station_number` — The unique ID of the weather station within the farm
- `temperature` — Air temperature at the station (°C or °F, as set in config)
- `humidity` — Air humidity percentage (%)
- `wind_speed` — Wind speed (e.g. km/h or m/s, as set in config)
- `wind_direction` — Wind direction (e.g. N, NE, S, etc.)
- `rainfall` — Amount of rain measured (e.g. mm)
- `weather_conditions` — General weather description (e.g. clear, cloudy, rain)
- `heat_index` — This combines air temperature and humidity to show how hot it actually feels to people
- `pressure` — Atmospheric pressure (e.g. hPa)
- *... or any other fields we choose in the config*

**Example before preprocessing (meteo):**
```python
{
  'source': 'Meteo',
  'city': 'Gazi (Athens)',
  'timecrawl': '2025-07-29 01:13:36',
  'last_station_update': '29/07/2025 01:00',
  'farm_number': 'farm3',
  'station_number': 1,
  'temperature': '28.8 °C',
  'humidity': '47 %',
  'wind_speed': '0.0 Km/h',
  'wind_direction': 'W',
  'rainfall': '0.0 mm',
  'weather_conditions': 'Clear',
  'heat_index': '29.6 °C',
  'pressure': '1012 hPa'
}

```

> The example below shows a raw record **before preprocessing**. After the preprocessing step, all values will be cleaned (units and symbols removed) and converted to the units we have selected in the config file ('results_units'). For example, temperatures will be converted to Celsius or Fahrenheit as specified, wind speeds to km/h or m/s, etc. Also, last_station_update will be stored as a standard timestamp.

**After preprocessing (meteo):**
```python
{
  'source': 'Meteo',
  'city': 'Gazi (Athens)',
  'timecrawl': '2025-07-29 01:13:36',
  'last_station_update': '2025-07-29 01:00:00',
  'farm_number': 3,
  'station_number': 1,
  'temperature': 28.8,
  'humidity': 47,
  'wind_speed': 0.0,
  'wind_direction': 'W',
  'rainfall': 0.0,
  'weather_conditions': 'Clear',
  'heat_index': 29.6,
  'pressure': 1012
}
```

## How To Run
  (TODO)
  
To run the project, you first need to have Python and Scrapy installed.

- Download and install `Python`: [python.org](https://www.python.org/downloads/).
- Install `Scrapy`:

```bash
pip install scrapy
```

### Running a Spider

1. Open your terminal and navigate to the `/PROVATO` folder.

2. To see all the available spiders in your project, run:
   ```bash
   scrapy list
   ```

3. To run a specific spider, use:
  ```bash
  scrapy crawl <spider_name>
  ```
  Replace <spider_name> with the actual name of the spider you want to run, for example:
  ```bash
  scrapy crawl meteo_live_data
  ```
  ```bash
  scrapy crawl wu_live_data
  ```
